if [ "$#" != "1" ]; then
  echo "usage: $0 <path of alive.py>"
  exit 1
fi

while true; do
  python3 "$1" localhost:4040 /api/v1/applications
  # python3 "$1" 10.10.114.105:8080 /api/v1/applications
  if [ "$?" = "0" ]; then
    echo running
  else
    echo restart service
    T1=$(date +%Y%m%d%H%M%S)
    nohup bash -c "spark-submit --jars /srv/spark/jars/spark-streaming-kafka-assembly_2.10-1.4.1.jar,/srv/spark/jars/elasticsearch-hadoop-2.1.1.jar submit.py network localhost 9999" >> "/home/xu/kselog$T1.log" 2>&1 &
    # nohup bash -c "/opt/spark/current/bin/spark-submit --jars /opt/spark/jars/spark-streaming-kafka-assembly_2.10-1.4.1.jar,/opt/spark/jars/elasticsearch-hadoop-2.1.1.jar --py-files /data/KSE/adapters.py submit.py kafka 10.10.157.227:2181 bzfun-app-log" >> "/data/KSELogs/kselog$T1.log" 2>&1 &
  fi
  sleep 45
done
