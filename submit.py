from __future__ import print_function
import sys
import json
import logging
from optparse import OptionParser, OptionGroup
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
    TTL = None

    # spark streaming
    INTERVAL = 1
    REMEMBER = None

    # es setting
    ES_NODES = None

    # app setting
    mode = None
    logpath = None
    hostname = None
    zkQuorum = None
    port = None
    topic = None

    # options
    usage = 'Usage: %prog <-m [mode]> [options]'
    mode_list = ['network', 'kafka']
    parser_system_options = [
        # system
        {
            "short": "-e",
            "long": "--esnode",
            "action": "store",
            "dest": "es",
            "default": None,
            "help": "elasticsearch node address",
            "type": "string"
        },
        {
            "short": "-l",
            "long": "--log",
            "action": "store",
            "dest": "log",
            "default": '/data/logs/KSE/',
            "help": "log path(endwith '/')",
            "type": "string"
        },
        {
            "short": "--spark-cleaner-ttl",
            "action": "store",
            "dest": "ttl",
            "default": "300",
            "help": "how long will data remain in memory(default 300 seconds)",
            "type": "string"
        },
        {
            "short": "--ssc-remember",
            "action": "store",
            "dest": "sscremember",
            "default": 240,
            "help": "how long will spark-streaming context remember \
                input data or persisted rdds.(default 240 seconds)",
            "type": "int"
        },
        {
            "short": "--interval",
            "action": "store",
            "dest": "interval",
            "default": 1,
            "help": "how long is the interval of spark-streaming \
                getting input data.(default 1 seconds)",
            "type": "int"
        },
    ]
    parser_options = [
        # mode
        {
            "short": "-m",
            "long": "--mode",
            "action": "store",
            "dest": "mode",
            "default": None,
            "help": "KSE mode",
            "type": "string"
        },
        {
            "short": "--hostname",
            "action": "store",
            "dest": "hostname",
            "default": None,
            "help": "network mode hostname",
            "type": "string"
        },
        {
            "short": "--port",
            "action": "store",
            "dest": "port",
            "default": None,
            "help": "network mode port",
            "type": "string"
        },
        {
            "short": "--zkquorum",
            "action": "store",
            "dest": "zkquorum",
            "default": None,
            "help": "kafka mode zkQuorum",
            "type": "string"
        },
        {
            "short": "--topic",
            "action": "store",
            "dest": "topic",
            "default": None,
            "help": "kafka mode topic",
            "type": "string"
        }
    ]

    def __init__(self, args):
        self.verify(args)

    def verify(self, args):
        parser = OptionParser(usage=self.usage)
        for opt in self.parser_options:
            parser.add_option(
                opt['short'],
                opt.get('long', None),
                action=opt['action'],
                dest=opt['dest'],
                default=opt['default'],
                help=opt['help'],
                type=opt['type'])
        group = OptionGroup(
            parser, "System Options",
            "Caution: These options usually use default values.")
        for opt in self.parser_system_options:
            group.add_option(
                opt['short'],
                opt.get('long', None),
                action=opt['action'],
                dest=opt['dest'],
                default=opt['default'],
                help=opt['help'],
                type=opt['type'])
        parser.add_option_group(group)
        (options, args) = parser.parse_args(args)
        self.TTL = options.ttl
        self.INTERVAL = options.interval
        self.REMEMBER = options.sscremember
        self.ES_NODES = options.es
        self.mode = options.mode
        self.logpath = options.log
        self.hostname = options.hostname
        self.zkQuorum = options.zkquorum
        self.port = options.port
        self.topic = options.topic
        if self.mode not in self.mode_list:
            parser.error("-m mast be one of [network, kafka]")
            exit(-1)
        if self.ES_NODES is None:
            parser.error("-e should not be null value")
            exit(-1)
        if self.mode == 'network' and (
                self.hostname is None or self.port is None):
            parser.error("--hostname and --port should not be null value")
            exit(-1)
        elif self.mode == 'kafka' and (
                self.zkQuorum is None or self.topic is None):
            parser.error("--zkquorum and --topic should not be null value")
            exit(-1)

    def getSparkConf(self):
        conf = SparkConf()
        conf.setAppName(self.PROJECT_NAME)
        conf.set(
            "spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.set("spark.cleaner.ttl", self.TTL)
        # es
        conf.set("es.index.auto.create", "true")
        conf.set("es.nodes", self.ES_NODES)
        return conf

    def getStreamingContext(self, sc):
        ssc = StreamingContext(sc, self.INTERVAL)
        ssc.remember(self.REMEMBER)
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
    # logger
    logger = logging.getLogger('output')
    d = datetime.now().strftime('%Y%m%d%H%M%S')
    f = logging.FileHandler("%s%s.log" % (conf.logpath, d))
    logger.addHandler(f)
    formatter = logging.Formatter(
        fmt="[%(levelname)s][%(asctime)s]%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    f.setFormatter(formatter)
    logger.setLevel(logging.INFO)

    main(sc, ssc, conf)
