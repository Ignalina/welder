package dk.ignalina.lab.spark301;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dk.ignalina.lab.spark301.base.EventSparkStreamingKafka;
import dk.ignalina.lab.spark301.base.Utils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class KafkaEventDrivenSparkJob extends EventSparkStreamingKafka {
    static Utils.Config config;

    public static String fire(ConsumerRecord<String, GenericRecord> record) {
        System.out.println("FIRE and action !!!!!!!!!!!!!!!1");

//        SparkSession spark = SparkSession.active();

        JsonParser parser = new JsonParser();
        String message = ""+record.value();

        System.out.println("Parse string to json object" + message);
        JsonObject jo = null;
        try {
            jo = parser.parse(message).getAsJsonObject();
        } catch (IllegalStateException ei) {
            System.out.println("Invalid unparsable json:" + ei.toString());
        }


        System.out.println(jo.toString());
        String filename = jo.get("body").getAsJsonObject().get("name").getAsString();
        System.out.println("Fick ett event med S3 fil och body.name=" + filename);
//        SparkSession s = SparkSession.active();
        return filename;
    }


    public static void main(String... args) {


        config = new Utils.Config(args);

        SparkConf conf = CreateSparkConf("v20220608 spark 3.0.1 streaming event job ");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(config.msIntervall));
        SparkSession spark = SparkSession.builder().getOrCreate();

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


//        JavaRDD<String> rddString;
/*
        stream.map(message -> message.value()).

                foreachRDD(javaRDD -> {
                    JavaRDD<Row> newRDD = javaRDD.map(x -> {
                        return Utils.avroToRowConverter(x, schemaStructured);
                    });


                    System.out.println(" ROW count=" + newRDD.count() + " antal ROW partitioner=" + newRDD.getNumPartitions() + "");
                    Dataset<Row> df2 = spark.createDataFrame(newRDD, schemaStructured);
                    df2.write().insertInto(config.topic);
                });
*/

        stream.foreachRDD(rdd -> {
            List<ConsumerRecord<String, GenericRecord>> c = rdd.collect();
            System.out.println("Gjorde collect" );

            c.forEach((record) -> {
                System.out.println("per record .." );

                System.out.println(record);
                String filename=fire(record);

                Dataset<Row> parquetFileDF = spark.read().parquet(filename);
                System.out.println("Överlevde session" );

                parquetFileDF.printSchema();

            });

//            rdd.foreach(record -> fire(record));
        });

        /*
        stream.foreachRDD(rdd -> {
            rdd.foreach(record -> fire(record));
        });
*/

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}
