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

import com.google.gson.JsonElement;
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

import java.util.*;

public class EventSparkStreamingKafka {

    public static Action actionImpl;

    public static SparkConf CreateSparkConf(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerialize");
        conf.registerKryoClasses((Class<ConsumerRecord>[]) Arrays.asList(ConsumerRecord.class).toArray());
        return conf;
    }

    public static void callForEachRdd(JavaRDD<ConsumerRecord<String, GenericRecord>> rdd) {
        System.out.println("HELLOOOO actionImpl=" + actionImpl);

        if (null == actionImpl) {
            return; //
        }

        JavaRDD<ConsumerRecord<String, GenericRecord>> rdd1 = rdd;
        System.out.println("rdd=" + rdd1.toDebugString());
        List<ConsumerRecord<String, GenericRecord>> rows = rdd1.collect();

        JsonParser parser = new JsonParser();

        for (ConsumerRecord<String, GenericRecord> cr : rows) {
            System.out.println("Parse string to json object" + cr.value());
            String message = ""+cr.value();
            JsonObject jo = null;
            try {
                jo = parser.parse(message).getAsJsonObject();
            } catch (IllegalStateException ei) {
                System.out.println("Invalid unparsable json:" + ei.toString());
            }

            if (jo != null) {
                actionImpl.fire(jo);
            }

        }
    }


}