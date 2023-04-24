#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
rm -rf /home/spark/.ivy2/cache
export SPARK_HOME=/opt/spark-3.3.2-bin-hadoop3
export VER=1.0-SNAPSHOT
#wget https://nexus.x14.se/repository/maven-releases/dk/ignalina/lab/spark332/welder-iceberg-solr-job/${VER}/welder-iceberg-solr-job-${VER}.jar
#wget https://s01.oss.sonatype.org/content/repositories/snapshots/dk/ignalina/lab/spark332/welder-iceberg-solr-job/${VER}/welder-iceberg-solr-job-${VER}
#--conf "spark.jars.ivySettings=/home/spark/.ivy2/ivysettings.xml" \

${SPARK_HOME}/bin/spark-submit \
--conf "spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-11-openjdk" \
--conf "spark.jars.packages=org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.54.0,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0,software.amazon.awssdk:sts:2.20.18,software.amazon.awssdk:s3:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18" \
--conf "spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider" \
--conf "spark.local.dir=/var/lib/x14/spark/spark.local.dir" \
--conf "spark.eventLog.dir=/var/lib/x14/spark/eventLog.dir" \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--class dk.ignalina.lab.spark332.CsvToIceberg \
--repositories https://nexus.x14.se/repository/maven-public,https://nexus.x14.se/repository/maven-releases,https://s01.oss.sonatype.org/content/repositories/snapshots \
--packages org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.54.0,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.0,software.amazon.awssdk:sts:2.20.18,software.amazon.awssdk:s3:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18 \
--driver-memory 4g \
--master spark://10.1.1.93:7077 \
--driver-cores 4 \
--verbose \
--deploy-mode cluster \
--supervise \
https://s01.oss.sonatype.org/content/repositories/snapshots/dk/ignalina/lab/spark332/welder-iceberg-solr-job/1.0-SNAPSHOT/welder-iceberg-solr-job-1.0-20230424.180313-4.jar \
testar kafka 10.1.1.90:9092 10.1.1.90:8081 testar-value /tmp/spark/checkpoint latest PERMISSIVE 1000 groupid labb password s3api.x14.se main 10.1.1.93:19120 authtype csv s3a://data/piwik/piwik.csv s3a://data/piwik/landing &

#--packages dk.ignalina.lab.spark301:welder-spark-job:0.0.15,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-streaming_2.12:3.0.1 \

#--packages dk.ignalina.lab.spark301:welder-spark-job:0.0.3,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.3,org.apache.spark:spark-streaming_2.12:3.0.3 \
