/*
 * MIT No Attribution
 *
 * Copyright 2021 Rickard Lundin (rickard@ignalina.dk)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package dk.ignalina.lab.spark301.base;

//import com.databricks.spark.avro.SchemaConverters;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;


public class Utils {

    public static class Config {
        public String topic = "topic";
        public String format;
        public String bootstrap_servers = "10.1.1.90:9092";
        public String schemaRegistryURL = "10.1.1.90:8081";
        public String subjectValueName;
        public String startingOffsets;
        public String checkpointDir;
        public String mode = "PERMISSIVE";
        public String failOnDataLoss = "false";
        public String master = "local[2]";
        public int msIntervall = 2000;
        public Object groupId="groupid";
        public Object autoOffsetReset="latest";
        public String s3AccessKey;
        public String s3SecretKey;
        public String s3EndPoint;
        public  Config(String[] args) {
            topic = args[0];
            format = args[1];
            bootstrap_servers = args[2];
            schemaRegistryURL = args[3];
            subjectValueName = args[4];
            checkpointDir = args[5];
            startingOffsets = args[6];
            mode = args[7];
            msIntervall = Integer.parseInt(args[8]); ;
            groupId=args[9];
            s3AccessKey=args[10];
            s3SecretKey=args[11];
            s3EndPoint=args[12];
        }

    }



    static public void createHiveTable(Dataset<Row> df, String tableName, SparkSession spark) {
        spark.sql("use hiveorg_prod");


        String tables[] = spark.sqlContext().tableNames();
        if (Arrays.asList(tables).contains(tables)) {
            return;
        }

        String tmpTableName = "my_temp" + tableName;

        df.createOrReplaceTempView(tmpTableName);
        spark.sql("drop table if exists " + tableName);
        spark.sql("create table " + tableName + " as select * from " + tmpTableName);

    }

    static public Dataset<Row> createEmptyRDD(SparkSession spark, StructType schema) {
        return spark.createDataFrame(new ArrayList<>(), schema);
    }
    public static SparkConf CreateSparkConf(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        return conf;
    }

    static public SparkSession createS3SparkSession(Utils.Config config) {
        SparkSession spark = SparkSession.builder().master(config.master).
                config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem").
                config("fs.s3a.access.key",config.s3AccessKey).
                config("fs.s3a.secret.key",config.s3SecretKey).
                config("fs.s3a.endpoint", config.s3EndPoint).
                config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem").
                config("fs.s3a.path.style.access","true").
                getOrCreate();
    return spark;
    }
    static public JavaInputDStream<ConsumerRecord<String, GenericRecord>> createStream(Utils.Config config,JavaStreamingContext ssc) {

        Map<String, Object> kafkaParams = new HashMap<>();

        kafkaParams.put("bootstrap.servers", config.bootstrap_servers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", config.groupId);
        kafkaParams.put("auto.offset.reset", config.startingOffsets);
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList(config.topic);

        JavaInputDStream<ConsumerRecord<String, GenericRecord>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, GenericRecord>Subscribe(topics, kafkaParams)
                );

        return stream;
    }


}
