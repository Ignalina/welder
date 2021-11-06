package dk.ignalina.lab.spark232;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.RowFactory;
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

import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
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
private static String avroSchema = null;
private static StructType schemaStructured = null;

    static  {

        try {
            avroSchema = Utils.getLastestSchema("http://10.1.1.90:8081","table_X14-value");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }

        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(avroSchema);
// TODO checkout https://stackoverflow.com/questions/40789153/how-to-convert-avro-schema-object-into-structtype-in-spark
        recordInjection = GenericAvroCodecs.toBinary(schema);


    }

    static Utils.Config config;


    public static void main(String... args) {
        System.out.println("v202111032343 spark 2.3.2 RDD kakfa V0.10+ streaming to hive");
        config = Utils.parToConfig(args);


      SparkConf conf = new SparkConf().setAppName("v202111032342  spark 2.3.2 streaming job").
              setMaster("spark://10.1.1.190:6066").
                set("spark.sql.warehouse.dir", "/apps/hive/warehouse").
                set("spark.submit.deployMode" , "cluster").
                set("spark.sql.catalogImplementation","hive").
                set("hive.metastore.uris","thrift://10.1.1.190:9083").
                set("spark.driver.supervise","true").
                set("spark.hadoop.fs.default.name", "hdfs://10.1.1.190:9000").
                set("spark.hadoop.fs.defaultFS", "hdfs://10.1.1.190:9000").
                set("spark.hadoop.fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()).
                set("spark.hadoop.fs.hdfs.server", org.apache.hadoop.hdfs.server.namenode.NameNode.class.getName()).
                set("spark.hadoop.conf", org.apache.hadoop.hdfs.HdfsConfiguration.class.getName());

        schemaStructured = Utils.avroToSparkSchema(schema);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(2000));
        SparkSession spark = SparkSession.builder().
                appName("v202111032342  spark 2.3.2 streaming job").
                config("spark.sql.warehouse.dir", "/apps/hive/warehouse").
                master("spark://10.1.1.190:6066").
                config("spark.submit.deployMode","cluster").
                enableHiveSupport().
                getOrCreate();





        Dataset<Row> df= Utils.createEmptyRDD(spark,schemaStructured);

        Utils.createHiveTable(df,config.topic,spark);

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
                foreachRDD( javaRDD -> {
                    JavaRDD<Row> rddOfRows =javaRDD.map(fields -> RowFactory.create(fields));
                    Dataset<Row> df2 =  spark.createDataFrame(rddOfRows,schemaStructured);
                    df2.write().saveAsTable(config.topic);
        });


/**
 * Update offsets if sucess storing all external
 * NOTE: Still not covered the case with half failure saving external.
 *
 * */

        stream.foreachRDD( rdd -> {

            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            if(null== TaskContext.get()) {
                return;
            }
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

// Got this from Ram Ghadiyaram
    private static Row avroToRowConverter(GenericRecord avroRecord,StructType structType) {
        if (null == avroRecord) {
            return null;
        }

        Object[] objectArray = new Object[avroRecord.getSchema().getFields().size()];
//        StructType structType = (StructType) SchemaConverters.toSqlType(avroRecord.getSchema()).dataType();
        for (Schema.Field field : avroRecord.getSchema().getFields()) {
            if(field.schema().getType().toString().equalsIgnoreCase("STRING") || field.schema().getType().toString().equalsIgnoreCase("ENUM")){
                objectArray[field.pos()] = ""+avroRecord.get(field.pos());
            }else {
                objectArray[field.pos()] = avroRecord.get(field.pos());
            }
        }

        return new GenericRowWithSchema(objectArray, structType);
    }
}
