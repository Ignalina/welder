#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
rm -rf /home/spark/.ivy2/cache
export SPARK_HOME=/opt/spark-3.3.2-bin-hadoop3
export VER=1.0-SNAPSHOT
#--conf "spark.jars.ivySettings=/home/spark/.ivy2/ivysettings.xml" \


${SPARK_HOME}/bin/spark-submit \
--driver-java-options="--add-exports java.base/sun.nio.ch=ALL-UNNAMED" \
--properties-file spark-defaults.properties \
--class dk.ignalina.lab.spark332.CsvToIceberg \
--driver-memory 4g \
--master spark://10.1.1.93:7077 \
--driver-cores 4 \
--verbose \
--deploy-mode cluster \
--supervise \
https://s01.oss.sonatype.org/content/repositories/snapshots/dk/ignalina/lab/spark332/welder-iceberg-solr-job/1.0-SNAPSHOT/welder-iceberg-solr-job-1.0-20230425.004349-7.jar \
testar kafka 10.1.1.90:9092 10.1.1.90:8081 testar-value /tmp/spark/checkpoint latest PERMISSIVE 1000 groupid labb password 10.1.1.68:9000 main 10.1.1.93:19120 authtype csv s3a://data/piwik/piwik.csv s3a://data/piwik/landing &

#--packages dk.ignalina.lab.spark301:welder-spark-job:0.0.15,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-streaming_2.12:3.0.1 \

#--packages dk.ignalina.lab.spark301:welder-spark-job:0.0.3,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.3,org.apache.spark:spark-streaming_2.12:3.0.3 \
