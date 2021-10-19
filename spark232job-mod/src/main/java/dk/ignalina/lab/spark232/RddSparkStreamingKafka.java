package dk.ignalina.lab.spark232;


import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;



import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RddSparkStreamingKafka {

private static Injection<GenericRecord, byte[]> recordInjection;

    static {
        String jsonFormatSchema = null;

        try {
            jsonFormatSchema = Utils.getLastestSchema("http://10.1.1.90:8081","topic-value");
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
                .setMaster("cluster");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Map<String, Object> kafkaParams = new HashMap<>();

        kafkaParams.put("bootstrap.servers", config.bootstrap_servers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("group.id", "groupid");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(config.topic);

        JavaInputDStream<ConsumerRecord<String, byte[]>> stream =
                KafkaUtils.createDirectStream(
                        ssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, byte[]>Subscribe(topics, kafkaParams)
                );


        stream.map(message -> recordInjection.invert(message.value()).get()).
                foreachRDD(rdd -> {
                    OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    rdd.foreachPartition(consumerRecords -> {
                        OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                        System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                        GenericRecord genericRecord=consumerRecords.next();

                        System.out.println("str1= " + genericRecord.get(1 )
                                + ", str2= " + genericRecord.get(2)
                                + ", int1=" +genericRecord.get(3));


                    });


            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
        });

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }


}
