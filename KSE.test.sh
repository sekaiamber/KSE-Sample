#!/bin/bash

# load utils
CurDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${CurDir}/utils.sh

# get host ip
HostIP="$(get_host_ip)"

# spark
Port=7077
WebuiPort=8080

# set data dir
KSEInclude=${CurDir}/jars
KSELogs='/data/KSE/logs'

update_images() {
  # pull spark-cluster docker image
  docker pull docker.baozou.com/baozou/spark:1.5-py3.4

  check_exec_success "$?" "pulling 'spark' image"
}

start() {

  # update_images

  mkdir -p ${KSELogs}

  # if previous docker container is not exit, kill it
  docker kill KSE 2>/dev/null
  docker rm KSE 2>/dev/null
  # get elasticsearch
  elasticsearchURL='172.17.0.2'
  # run job
  docker run -d --name KSE -p ${Port}:${Port} -p ${WebuiPort}:${WebuiPort} \
    -v ${KSEInclude}:/opt/spark/jars \
    -v ${CurDir}:/data/KSE \
    -v ${KSELogs}:/data/logs/KSE \
    --net=host \
    --log-opt max-size=100m \
    --log-opt max-file=9 \
    docker.baozou.com/baozou/spark:1.5-py3.4 \
    spark-submit --master local[*] \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.cleaner.ttl=300" \
    --conf "spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC" \
    --driver-java-options "-XX:+UseConcMarkSweepGC" \
    --files /data/KSE/log4j.properties \
    --jars /opt/spark/jars/spark-streaming-kafka-assembly.jar,/opt/spark/jars/elasticsearch-hadoop.jar \
    --py-files /data/KSE/adapters.py /data/KSE/submit.py network 172.17.42.1 9999 ${elasticsearchURL} \
    2>&1

  check_exec_success "$?" "submit project KSE"
}

stop() {
  docker kill KSE 2>/dev/null
  docker rm KSE 2>/dev/null
}

info() {
  SparkMasterURL=""
  curl ${SparkMasterURL}/api/v1/applications
}

destroy() {
  echo "destroy..."
}

##################
# Start of script
##################

case "$1" in
  start) start ;;
  stop) stop ;;
  info) info ;;
  restart)
    stop
    start
    ;;
  destroy) destroy ;;
  *)
    echo "Usage: ./KSE.sh start|stop|info|restart"
    exit 1
    ;;
esac

exit 0
