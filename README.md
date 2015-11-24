# KSE

## Introduction

Kafka-Spark streaming-Elasticsearch. This project is used for getting data from Kafka and dealing through Spark streaming and finally storing into Elasticsearch.

## Version

version is built in this format: vx.y[.z]

x: API version changed, tech stack changed
y: New feature/api introduced, Code architecture changed
z: bug fixed, small feature/improvment

## Usage

```shell
$ spark-submit submit.py --help
Usage: submit.py <-m [mode]> [options]

Options:
  -h, --help            show this help message and exit
  -m MODE, --mode=MODE  KSE mode
  --hostname=HOSTNAME   network mode hostname
  --port=PORT           network mode port
  --zkquorum=ZKQUORUM   kafka mode zkQuorum
  --topic=TOPIC         kafka mode topic

  System Options:
    Caution: These options usually use default values.

    -e ES, --esnode=ES  elasticsearch node address
    -l LOG, --log=LOG   log path(endwith '/')
    --checkpoint        enable spark checkpoint
    --checkpoint-path=CHECKPOINTPATH
                        spark checkpoint directory(endwith '/')
    --checkpoint-interval=CHECKPOINTINTERVAL
                        spark checkpoint interval(default 10 seconds)
    --spark-cleaner-ttl=TTL
                        how long will data remain in memory(default 300
                        seconds)
    --ssc-remember=SSCREMEMBER
                        how long will spark-streaming context remember
                        input data or persisted rdds.(default 240 seconds)
    --ssc-window=SSCWINDOW
                        how long will spark-streaming context window
                        be. Note this must be multiples of the batch interval
                        of the value --ssc-interval.(default 10 seconds)
    --ssc-interval=INTERVAL
                        how long is the interval of spark-streaming
                        getting input data.(default 2 seconds)
```
Common options:

| parameter | description | example |
| ---- | ---- | ---- |
| -m(--mode) | where we got the data, limited in [network, kafka], **if this set to `network`, `--hostname` and `--port` should not be null value, if this set to `kafka`, `--zkquorum` and `--topic` should not be null value**. | / |
| --hostname | In `network` mode, hostname | localhost; 172.22.13.51 |
| --zkquorum | In `kafka` mode, zookeeper quorum | 10.10.157.227:2181 |
| --port | In `network` mode, port | 80; 8080 |
| --topic | In `kafka` mode, topic | your-topic-in-kafka |

System options:

| parameter | description | example |
| ---- | ---- | ---- |
| -e(--esnode) | List of Elasticsearch nodes to connect to. **Note that the list does not have to contain every node inside the Elasticsearch cluster**. | 172.17.0.1; 172.17.0.1:9200; 172.17.0.1:9200,172.17.0.2:9200 |
| -l(--log) | Log path(endwith '/'). **Note that this log is KSE output, not spark itself**. | / |
| --checkpoint | Enable spark checkpoint | / |
| --checkpoint-path | Spark checkpoint directory(endwith '/') | / |
| --checkpoint-interval | Spark checkpoint interval(default 10 seconds) | / |
| --spark-cleaner-ttl | how long will data remain in memory(default 300 seconds) | / |
| --ssc-remember | how long will spark-streaming context remember input data or persisted rdds.(default 240 seconds) | / |
| --ssc-interval | how long is the interval of spark-streaming getting input data.(default 2 seconds) | / |
| --ssc-window | how long will spark-streaming context window be. Note this must be multiples of the batch interval of the value --ssc-interval.(default 10 seconds) | / |

**Submit example**

```shell
# spark-submit --jars /srv/spark/jars/spark-streaming-kafka-assembly_2.10-1.4.1.jar,/srv/spark/jars/elasticsearch-hadoop-2.1.1.jar --py-files /home/xu/work/KSE/adapters.py submit.py -m network --hostname=172.17.0.1 --port=9999 -e 172.17.0.1 --spark-cleaner-ttl=300 --ssc-remember=240
```

## Using start script

We are using [etcd](https://github.com/coreos/etcd) for service discovery. So usually we get elasticsearch cluster's address, spark master host address and zookeeper host address from etcd. And we use [docker](http://www.docker.com/) to manager our environment.  
In this case, we built a script to start KSE. See `KSE.sh` and `KSE.test.sh`.

```shell
$ ./KSE.test.sh
Usage: ./KSE.sh start|stop|info|restart|destroy
```

`start` command will automatically pull docker image, make work, log and checkpoint directorys, start docker container and start KSE server.  
`stop` command will stop docker container and remove it.  
`info` command will list some useful informations.  
`restart` command is combine of `stop` and `start`.  
`destroy` command will call `stop` first and remove all directorys.