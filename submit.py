from __future__ import print_function
import sys
import json
import logging
from datetime import datetime

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import adapters as AllAdapters


###############
# Config class
###############
class Config(object):
    # spark setting
    PROJECT_NAME = 'KSE'
    EXECUTOR_MEMORY = '1g'

    # spark streaming
    INTERVAL = 1

    # es setting
    ES_NODES = None

    # app setting
    mode = None
    hostname = None
    zkQuorum = None
    port = None
    topic = None
    debug = True

    mode_list = ['network', 'kafka']

    def __init__(self, args):
        if len(args) != 5:
            stdout("Usage: submit.py <mode> \
                <hostname or zkQuorum> <port or topic> \
                <elasticsearch node url>", file=sys.stderr)
            exit(-1)
        self.mode = args[1]
        if self.mode not in self.mode_list:
            stdout("<mode> mast be one of [network, kafka]", file=sys.stderr)
            exit(-1)
        if self.mode == 'network':
            self.hostname, self.port = args[2:4]
        elif self.mode == 'kafka':
            self.zkQuorum, self.topic = args[2:4]
        self.ES_NODES = args[4]

    def getSparkConf(self):
        conf = SparkConf()
        conf.setAppName(self.PROJECT_NAME)
        conf.set(
            "spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.cleaner.ttl", "300")
        # es
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", self.ES_NODES)
        return conf

    def getStreamingContext(self, sc):
        ssc = StreamingContext(sc, self.INTERVAL)
        ssc.remember(240)
        return ssc


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
    ES_NODES = None

    def __init__(self, nodes):
        self.ES_NODES = nodes

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
        except Exception as e:
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
# Output
###############
def output(msg):
    logger = logging.getLogger('output')
    logger.info(msg)


def stdout(msg):
    print(msg)


###############
# Deal logic
###############
def saveEachRDD(rdd, adapter, conf):
    if rdd.isEmpty():
        output("[%s] 0" % adapter.es_doc_type)
        return
    count = rdd.count()
    EsClient(conf.value.ES_NODES).saveTOES(
        rdd,
        index=adapter.es_prefix + datetime.now().strftime('%Y.%m.%d'),
        doc_type=adapter.es_doc_type)
    output("[%s] %d" % (adapter.es_doc_type, count))


def dealeach(adapter, lines, conf):
    doing = lines.flatMap(lambda line: adapter.act(line))
    doing = doing.map(lambda item: ('key', item))
    doing.foreachRDD(lambda rdd: saveEachRDD(rdd, adapter, conf))


def deal(lines, conf):
    adapters = [
        # AllAdapters.BaseAdapter(),
        AllAdapters.searchClickAdapter(),
        AllAdapters.recommendclickAdapter(),
        AllAdapters.playonlineAdapter(),
        AllAdapters.stickyseriesAdapter(),
        AllAdapters.viewcountAdapter()
    ]
    for adapter in adapters:
        dealeach(adapter, lines, conf)


###############
# Main logic
###############
def networkReader(ssc, conf):
    lines = ssc.socketTextStream(conf.hostname, int(conf.port))
    return lines


def kafkaReader(ssc, conf):
    kvs = KafkaUtils.createStream(
        ssc,
        conf.zkQuorum,
        "spark-streaming-log",
        {conf.topic: 1}
    )
    lines = kvs.map(lambda x: x[1])
    return lines


def jsonDecode(line):
    try:
        return logDecoder().decode(line)
    except Exception as e:
        return {}


def Handler(lines, conf):
    lines = lines.map(lambda line: jsonDecode(line))
    deal(lines, conf)


def main(sc, ssc, conf):
    if conf.mode == 'network':
        lines = networkReader(ssc, conf)
    elif conf.mode == 'kafka':
        lines = kafkaReader(ssc, conf)
    conf = sc.broadcast(conf)
    Handler(lines, conf)

    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    conf = Config(sys.argv)
    spConf = conf.getSparkConf()
    sc = SparkContext(conf=spConf)
    ssc = conf.getStreamingContext(sc)
    logger = logging.getLogger('output')
    d = datetime.now().strftime('%Y%m%d%H%M%S')
    f = logging.FileHandler("/data/logs/KSE/%s.log" % d)
    logger.addHandler(f)
    formatter = logging.Formatter(
        fmt="[%(levelname)s][%(asctime)s]%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    f.setFormatter(formatter)
    logger.setLevel(logging.INFO)

    main(sc, ssc, conf)
