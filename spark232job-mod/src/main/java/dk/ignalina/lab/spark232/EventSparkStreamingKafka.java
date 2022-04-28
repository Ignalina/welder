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

package dk.ignalina.lab.spark232;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.AvroDefault;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class EventSparkStreamingKafka {


    static Utils.Config config;


    public static void main(String... args) {
        System.out.println("About to start streaming evenvt job");

        config = Utils.parToConfig(args);


        SparkConf conf = new SparkConf().setAppName("v20220425 spark 2.3.2 streaming event job ");

        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerialize");
        conf.registerKryoClasses((Class<ConsumerRecord>[]) Arrays.asList(ConsumerRecord.class).toArray());

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));
        SparkSession spark = SparkSession.builder().getOrCreate();


        Map<String, Object> kafkaParams = new HashMap<>();

        kafkaParams.put("bootstrap.servers", config.bootstrap_servers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);

        kafkaParams.put("group.id", "groupid");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);

        Collection<String> topics = Arrays.asList(config.topic);

        JavaInputDStream<ConsumerRecord<String, GenericRecord>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, GenericRecord>Subscribe(topics, kafkaParams)
                );


        JavaRDD<String> rddString;

        stream.foreachRDD(EventSparkStreamingKafka::callForEachRdd);

        System.out.println("About to start ssc");
        ssc.start();
        try {
            System.out.println("About to await term");
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("WE ARE DONE HA HA HA");


    }

    private static void callForEachRdd(JavaRDD<ConsumerRecord<String, GenericRecord>> rdd) {
        JavaRDD<ConsumerRecord<String, GenericRecord>> rdd1 = rdd;
        System.out.println("rdd=" + rdd1.toDebugString());
        List<ConsumerRecord<String, GenericRecord>> rows = rdd1.collect();
        for (ConsumerRecord<String, GenericRecord> cr : rows) {
            System.out.println("TODO Extract filename and use this to read the parquet file  !" + cr.value());
        }
    }
}