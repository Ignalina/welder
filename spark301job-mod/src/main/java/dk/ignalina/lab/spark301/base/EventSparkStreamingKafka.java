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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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

public class EventSparkStreamingKafka {

    public static Action actionImpl;

    public static SparkConf CreateSparkConf(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        return conf;
    }


    public static void callForEachRecord(ConsumerRecord<String, GenericRecord> record, SparkSession spark) {

        if (null == actionImpl) {
            return; //
        }

        JsonParser parser = new JsonParser();
        String message = ""+record.value();

        System.out.println("Parse string to json object" + message);
            JsonObject jo = null;
            try {
                jo = parser.parse(message).getAsJsonObject();
            } catch (IllegalStateException ei) {
                System.out.println("Invalid unparsable json:" + ei.toString());
            }

            if (jo != null) {
                actionImpl.fire(jo,spark);
            }

    }

    public static JavaStreamingContext getStreamingContext(Utils.Config config) {
        SparkConf conf = EventSparkStreamingKafka.CreateSparkConf("v20220610 spark 3.0.1 streaming event job ");

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
        stream.foreachRDD(rdd -> {
            rdd.foreach(record -> callForEachRecord(record,spark));
        });

        return ssc;
    }


}