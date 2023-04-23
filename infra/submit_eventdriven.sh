#!/bin/bash
rm -rf /home/spark/.ivy2/cache
export SPARK_HOME=/opt/spark-2.3.2-bin-hadoop2.7

$SPARK_HOME/bin/spark-submit \
--conf "spark.jars.ivySettings=/home/spark/.ivy2/ivysettings.xml" \
--class dk.ignalina.lab.spark232.EventSparkStreamingKafka \
--repositories https://nexus.x14.se/repository/maven-public,https://nexus.x14.se/repository/maven-relase \
--packages org.apache.avro:avro:1.9.2,com.databricks:spark-avro_2.11:4.0.0,dk.ignalina.lab.spark232:welder-spark-job:0.0.2,org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.2,org.apache.spark:spark-streaming_2.11:2.3.2 \
--driver-memory 4g \
--master spark://10.1.1.190:6067 \
--driver-cores 4 \
--verbose \
--deploy-mode cluster \
/home/spark/ksmb.jar \
testar kafka 10.1.1.90:9092 10.1.1.90:8081 testar-value /tmp/spark/checkpoint latest PERMISSIVE &
