package dk.ignalina.lab.spark232;


import java.io.IOException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;
*/

/**
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
*/

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.serializer.StringDecoder;

import org.apache.avro.Schema;
import com.twitter.bijection.avro.GenericAvroCodecs;
import com.twitter.bijection.Injection;

import java.util.*;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import scala.Tuple2;


import java.util.*;

public class RddSparkStreamingKafka {

private static Injection<GenericRecord, byte[]> recordInjection;

    static {
        String jsonFormatSchema = null;

        try {
            jsonFormatSchema = Utils.getLastestSchema("10.1.1.90:8081","topic");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(jsonFormatSchema);

        recordInjection = GenericAvroCodecs.toBinary(schema);
    }

    static Utils.Config config;


    public static void main(String... args) {
        System.out.println("spark 2.3.2 RDD  streaming");
        config = Utils.parToConfig(args);

        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        kafkaParams.put("specific.avro.reader", "true");

//        kafkaParams.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
//        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", KafkaAvroDeserializer.class);

//        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics);
        Collection<String> topics = Arrays.asList(config.topic);

        JavaInputDStream<ConsumerRecord<String, byte[]>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, byte[]>Subscribe(topics, kafkaParams)
                );

        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
        });






    }


}
