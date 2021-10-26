package dk.ignalina.lab.spark232;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;


import java.io.IOException;
import java.util.*;

public class RddSparkStreamingKafka {

private static Injection<GenericRecord, byte[]> recordInjection;
private static Schema schema;

    static {
        String jsonFormatSchema = null;

        try {
            jsonFormatSchema = Utils.getLastestSchema("http://10.1.1.90:8081","table_X14-value");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }

        Schema.Parser parser = new Schema.Parser();
        recordInjection = GenericAvroCodecs.toBinary(schema);
        schema = parser.parse(jsonFormatSchema);


    }

    static Utils.Config config;


    public static void main(String... args) {
        System.out.println("spark 2.3.2 RDD  streaming");
        config = Utils.parToConfig(args);



        SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("spark://10.1.1.190:6067");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.getActiveSession().get();// builder.config(sc.getConf).getOrCreate()

// Trick to get Schema in StructType form (not silly Avro form) for later Dataset creation.
        Dataset<Row> df = spark.read().format("avro").option("avroSchema", schema.toString()).load();
        StructType schemaStructured=  df.schema();


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
// Note bad naming .. javaRDD.rdd() gives proper rdd ?!
            foreachRDD( javaRDD -> {
// None of line 95,96 compiles                     
//  Possible alternative to get Schema in non avro form:   com.databricks.spark.avro.SchemaConverters.toSqlType(schema);
                        Dataset<GenericRecord> df1 = spark.sqlContext().createDataset(javaRDD.rdd(),);
                        Dataset<Row> df2 =  spark.createDataFrame(javaRDD,schemaStructured);
        });
/**
 * Update offsets if sucess storing all external
 * NOTE: Still not covered the case with half failure saving external.
 *
 * */

        stream.foreachRDD( rdd -> {

            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
            System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());

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
