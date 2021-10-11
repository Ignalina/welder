package dk.ignalina.lab;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import io.confluent.kafka.schemaregistry.client.rest.RestService;


import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.col;


public class AppSparkStreamingKafka {
    static Utils.Config config;


    public static void main(String[] args) {
        config=Utils.parToConfig(args);

        SparkSession spark;

        spark = SparkSession.builder()
                    .master("local")
                .appName("Welder")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> ds = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.bootstrap_servers)
                .option("subscribe", config.topic )
                .option("startingOffsets", config.startingOffsets) // From starting
                .load();

        ds.printSchema();

        String jsonFormatSchema = null;
        Dataset<Row> rowDS=null;

        try {
            jsonFormatSchema = Utils.getLastestSchema(config);
            rowDS= ds.select(from_avro(col("value"), jsonFormatSchema).as("row")).select("row.*");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
        StreamingQuery streamingQuery=null;

        try {
            streamingQuery=rowDS.writeStream().format("console").outputMode("append").trigger(Trigger.ProcessingTime("1 second"))  .option("checkpointLocation", config.checkpointDir).start();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }


        try {
            streamingQuery.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }


    }


}
