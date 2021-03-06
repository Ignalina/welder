package dk.ignalina.lab.spark232;

import com.google.gson.JsonObject;
import dk.ignalina.lab.spark232.base.Action;
import dk.ignalina.lab.spark232.base.EventSparkStreamingKafka;
import dk.ignalina.lab.spark232.base.Utils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class HelloWorld extends EventSparkStreamingKafka {

    static Utils.Config config;



    public static void main(String... args) {

        actionImpl=new Action() {
            @Override
            public boolean fire(JsonObject jsonObject) {
                System.out.println(jsonObject.toString());
                return true;
            }
        };

        config = new Utils.Config(args);

        SparkConf conf = CreateSparkConf("v20220428 spark 2.3.2 streaming event job ");

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


        JavaRDD<String> rddString;

        stream.foreachRDD(HelloWorld::callForEachRdd);

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}
