from __future__ import print_function
import sys
import re
import json
from datetime import datetime

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


###############
# Config class
###############

class Config(object):
    # spark setting
    PROJECT_NAME = 'KSE'
    # MASTER = 'spark://10.10.114.105:7077'
    MASTER = 'local[*]'
    EXECUTOR_MEMORY = '1g'

    # spark streaming
    INTERVAL = 1

    # es setting
    ES_NODES = '172.17.0.2'
    # ES_NODES = '10.10.50.105'

    # app setting
    mode = None
    hostname = None
    zkQuorum = None
    port = None
    topic = None
    debug = True

    mode_list = ['network', 'kafka']

    def __init__(self, args):
        if len(args) != 4:
            print("Usage: submit.py <mode> \
                <hostname or zkQuorum> <port or topic>", file=sys.stderr)
            exit(-1)
        self.mode = args[1]
        if self.mode not in self.mode_list:
            print("<mode> mast be one of [network, kafka]", file=sys.stderr)
            exit(-1)
        if self.mode == 'network':
            self.hostname, self.port = args[2:]
        elif self.mode == 'kafka':
            self.zkQuorum, self.topic = args[2:]

    def getSparkConf(self):
        conf = SparkConf()
        conf.setAppName(self.PROJECT_NAME)
        conf.setMaster(self.MASTER)
        # conf.set("spark.executor.memory", self.EXECUTOR_MEMORY)
        # es
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", self.ES_NODES)
        return conf

    def getStreamingContext(self, sc):
        return StreamingContext(sc, self.INTERVAL)


###############
# Json Decoder
###############

class logDecoder(json.JSONDecoder):

    def __init__(self):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object)

    def dict_to_object(self, d):
        for key in d:
            if isinstance(d[key], list):
                d[key] = tuple(d[key])
        return d


###############
# ES
###############

class EsClient(object):
    # es setting
    ES_NODES = '172.17.0.2'
    # ES_NODES = '10.10.50.105'

    def getESRDD(self, index=None, doc_type=None, query=None):
        if index is None or doc_type is None or self.ES_NODES is None:
            return None
        conf = {
            "es.resource": "%s/%s" % (index, doc_type),
            "es.nodes": self.ES_NODES,
        }
        if query is not None:
            conf["es.query"] = json.dumps(query)
        try:
            es_rdd = self.SC.newAPIHadoopRDD(
                inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
                keyClass="org.apache.hadoop.io.NullWritable",
                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                conf=conf)
            return es_rdd
        except Exception, e:
            log(e, level='ERROR')
            return None

    def saveTOES(self, rdd, index=None, doc_type=None):
        if index is None or doc_type is None or self.ES_NODES is None:
            return False
        conf = {
            "es.resource": "%s/%s" % (index, doc_type),
            "es.nodes": self.ES_NODES,
        }
        rdd.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=conf)
        return True


###############
# Adapters
###############

class BaseAdapter(object):

    grok_pattern = r'.+'
    es_prefix = 'debug-logstash-bzfun-'
    es_doc_type = 'web'

    def act(self, source):
        if 'path' not in source:
            return []
        g = self.grok(source['path'])
        if g is None:
            return []
        source = self.pretreat(source, extra=g)
        source = self.build(source)
        return [source]

    def grok(self, path):
        m = re.match(self.grok_pattern, path)
        if m is None:
            return None
        else:
            return m.groupdict()

    def pretreat(self, source, extra={}):
        for key in extra:
            source[key] = extra[key]
        return source

    def build(self, source):
        return source


class searchClickAdapter(BaseAdapter):

    grok_pattern = r'/ping/sc/?'
    es_prefix = 'debug-logstash-searchclick-'
    es_doc_type = 'searchclick'

    def build(self, source):
        ret = {
            '@timestamp': source.get('@timestamp', None),
            'type': 'searchclick',
            'q': None,
            'sid': None,
            'path': source.get('path', None)
        }
        para = source.get('parameters', None)
        if para is None:
            return ret
        ret['q'] = para.get('q', None)
        ret['sid'] = para.get('sid', None)
        return ret


def saveEachRDD(rdd, adapter):
    if rdd.isEmpty():
        print("%s 0, %s" % (adapter.es_doc_type, datetime.now().strftime('%M:%S')))
        return
    count = rdd.count()
    EsClient().saveTOES(rdd,
        index=adapter.es_prefix + datetime.now().strftime('%Y.%m.%d'),
        doc_type=adapter.es_doc_type)
    print("%s %d, %s" % (adapter.es_doc_type, count, datetime.now().strftime('%M:%S')))


def dealeach(adapter, lines):
    doing = lines.flatMap(lambda line: adapter.act(line))
    doing = doing.map(lambda item: ('key', item))
    doing.foreachRDD(lambda rdd: saveEachRDD(rdd, adapter))


def deal(lines, conf):
    adapters = [BaseAdapter(), searchClickAdapter()]
    for adapter in adapters:
        dealeach(adapter, lines)

###############
# Main logic
###############

def networkReader(ssc, conf):
    lines = ssc.socketTextStream(conf.hostname, int(conf.port))
    return lines


def kafkaReader(ssc, conf):
    kvs = KafkaUtils.createStream(ssc, conf.zkQuorum, "spark-streaming-log", {conf.topic: 1})
    lines = kvs.map(lambda x: x[1])
    return lines

def jsonDecode(line):
    try:
        return logDecoder().decode(line)
    except Exception, e:
        return {}


def Handler(lines, conf):
    lines = lines.map(lambda line: jsonDecode(line))
    deal(lines, conf)


def main(sc, ssc, conf):
    if conf.mode == 'network':
        lines = networkReader(ssc, conf)
    elif conf.mode == 'kafka':
        lines = kafkaReader(ssc, conf)
    # counts = lines.flatMap(lambda line: line.split(" "))\
    #               .map(lambda word: (word, 1))\
    #               .reduceByKey(lambda a, b: a+b)
    ES_NODES = sc.broadcast(conf.ES_NODES)
    Handler(lines, conf)

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    conf = Config(sys.argv)
    spConf = conf.getSparkConf()
    sc = SparkContext(conf=spConf)
    ssc = conf.getStreamingContext(sc)

    main(sc, ssc, conf)
