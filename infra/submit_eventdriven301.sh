#!/bin/bash
rm -rf /home/spark/.ivy2/cache

spark-submit \
--conf "spark.jars.ivySettings=/home/spark/.ivy2/ivysettings.xml" \
--class dk.ignalina.lab.spark301.KafkaEventDrivenSparkJob \
--repositories https://nexus.x14.se/repository/maven-public,https://nexus.x14.se/repository/maven-snapshots \
--packages dk.ignalina.lab.spark301:welder-spark-job:0.0.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-streaming_2.12:3.0.1 \
--driver-memory 4g \
--master spark://10.1.1.190:6067 \
--driver-cores 4 \
--verbose \
--deploy-mode cluster \
ksmb.jar \
testar kafka 10.1.1.90:9092 10.1.1.90:8081 testar-value /tmp/spark/checkpoint latest PERMISSIVE &
