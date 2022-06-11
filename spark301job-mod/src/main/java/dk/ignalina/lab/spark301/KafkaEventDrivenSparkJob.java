package dk.ignalina.lab.spark301;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dk.ignalina.lab.spark301.base.EventSparkStreamingKafka;
import dk.ignalina.lab.spark301.base.Utils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.codehaus.janino.Java;

import java.util.*;

public class KafkaEventDrivenSparkJob extends EventSparkStreamingKafka {
    static Utils.Config config;
    static JsonParser parser;

//    public static String extractFileName(ConsumerRecord<String, GenericRecord> record) {
    public static String extractFileName(ConsumerRecord<String, GenericRecord> record) {

        String message = ""+record.value();
        JsonObject jo = null;
        try {
            jo = parser.parse(message).getAsJsonObject();
        } catch (IllegalStateException ei) {
            String res="JSON CONTAINED NO PARSABLE FILENAME";
            System.out.println(res);
            return res;
        }
        String filename=jo.get("body").getAsJsonObject().get("name").getAsString();
        System.out.println("Filename extraced from JSON=" + filename);

        return filename;
    }


    public static void main(String... args) {


        config = new Utils.Config(args);
        parser = new JsonParser();

        SparkConf conf = CreateSparkConf("v20220612 spark 3.0.1 Streaming /event driven spark job");


        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(config.msIntervall));
        SparkSession spark = SparkSession.builder().master("spark://10.1.1.196:7077").
                config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem").
                config("fs.s3a.access.key",config.s3AccessKey).
                config("fs.s3a.secret.key",config.s3SecretKey).
                config("fs.s3a.endpoint", config.s3EndPoint).
                config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem").
                config("fs.s3a.path.style.access","true").
                getOrCreate();

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


        stream.foreachRDD(rdd -> {
                    JavaRDD<String> filenames = rdd.map(record -> extractFileName(record)); // VARNING/TODO: List needs to be sorted on date for correct Delta ingest order.

                    filenames.collect().forEach(fileName -> {
                        System.out.println("Filename from JSON=" + fileName);
                        Dataset<Row> df = spark.read().parquet(fileName);
                        df.printSchema();
                    });
                });


        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            // TODO Make sureKAFKA read offset is getting NOT updated  !
        }


    }

}
