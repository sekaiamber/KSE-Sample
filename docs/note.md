# note

## zookeeper

zkQuorum: 10.10.157.227:2181
topic: bzfun-app-log

## submit

```shell
# spark-submit --jars spark-streaming-kafka-assembly_2.10-1.4.1.jar
```

## kill spark applications

```shell
curl --data "id=app-20151012074225-0038&terminate=true" '10.10.114.105:8080/app/kill/'
```
