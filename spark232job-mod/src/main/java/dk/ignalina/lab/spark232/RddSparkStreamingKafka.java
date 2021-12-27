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

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RddSparkStreamingKafka {

    private static Injection<GenericRecord, byte[]> recordInjection;
    private static Schema schema;
    private static String avroSchema = null;
    private static StructType schemaStructured = null;

    static {

        try {
            avroSchema = Utils.getLastestSchema("http://10.1.1.90:8081", "table_X14-value");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }

        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(avroSchema);
        recordInjection = GenericAvroCodecs.toBinary(schema);
    }

    static Utils.Config config;


    public static void main(String... args) {
        config = Utils.parToConfig(args);


        SparkConf conf = new SparkConf().setAppName("v202111061807  spark 2.3.2 streaming job to hive").
                setMaster("spark://10.1.1.190:6066").
                set("spark.sql.warehouse.dir", "/apps/hive/warehouse").
                set("spark.submit.deployMode", "cluster").
                set("spark.sql.catalogImplementation", "hive").
                set("hive.metastore.uris", "thrift://10.1.1.190:9083").
                set("spark.driver.supervise", "true").
                set("spark.hadoop.fs.defaultFS", "hdfs://10.1.1.190:8020").
                set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()).
                set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName()).
                set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName());

        schemaStructured = Utils.avroToSparkSchema(schema);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));
        SparkSession spark = SparkSession.builder().
                appName("park 2.3.2 streaming job").
                config("spark.sql.warehouse.dir", "/apps/hive/warehouse").
                master("spark://10.1.1.190:6066").
                config("spark.submit.deployMode", "cluster").
                enableHiveSupport().
                getOrCreate();

        spark.sql("use hiveorg_prod");


        Dataset<Row> df = Utils.createEmptyRDD(spark, schemaStructured);

        Utils.createHiveTable(df, config.topic, spark);

        Map<String, Object> kafkaParams = new HashMap<>();

        kafkaParams.put("bootstrap.servers", config.bootstrap_servers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        kafkaParams.put("schema.registry.url", "http://10.1.1.90:8081");
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

        stream.map(message -> message.value()).
                foreachRDD(javaRDD -> {
                    JavaRDD<Row> newRDD = javaRDD.map(x -> {
                        return Utils.avroToRowConverter(x, schemaStructured);
                    });

                    System.out.println(" ROW count=" + newRDD.count() + " antal ROW partitioner=" + newRDD.getNumPartitions() + "");
                    Dataset<Row> df2 = spark.createDataFrame(newRDD, schemaStructured);
                    df2.write().insertInto(config.topic);
                });


/**
 * Update offsets if sucess storing all external
 * NOTE: Still not covered the case with half failure saving external.
 *
 * */

/**
 *  Temporary disable offset handling

 stream.foreachRDD( rdd -> {
 OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
 if(null== TaskContext.get()) {
 return;
 }
 OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
 System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
 ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
 });
 **/
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}
