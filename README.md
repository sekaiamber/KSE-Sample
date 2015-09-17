# KSE-Sample

## Introduction

This is a sample of Kafka-Spark streaming-Elasticsearch system.
This project use to get data from Kafka and deal through Spark streaming and finally store in Elasticsearch.

## Version

version is built in this format: vx.y[.z]

x: API version changed, tech stack changed
y: New feature/api introduced, Code architecture changed
z: bug fixed, small feature/improvment

## Usage

```shell
# spark-submit submit.py <mode> <hostname or zkQuorum> <port or topic>
```

| parameter | description | example |
| ---- | ---- | ---- |
| mode | where we got the data, limited in [network, kafka], **if this set to `network`, next 2 parameters will be hostname and port, if this set to `kafka`, next 2 parameters will be zkQuorum and topic**. | / |
| hostname | In `network` mode, hostname | localhost; 172.22.13.51 |
| zkQuorum | In `kafka` mode, zookeeper quorum | 10.10.157.227:2181 |
| port | In `network` mode, port | 80; 8080 |
| topic | In `kafka` mode, topic | your-topic-in-kafka |

**Submit example**

```shell
# spark-submit --jars /srv/spark/jars/spark-streaming-kafka-assembly_2.10-1.4.1.jar,/srv/spark/jars/elasticsearch-hadoop-2.1.1.jar submit.py network localhost 9999
```
