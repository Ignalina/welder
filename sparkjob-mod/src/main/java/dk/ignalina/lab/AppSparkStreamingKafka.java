package dk.ignalina.lab;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.RestService;


import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.col;


public class AppSparkStreamingKafka {
    class Config {
        public String topic="topic";
        public String format;
        public String bootstrap_servers="10.1.1.90:9092";
        public String subscribe;
        public String schemaRegistryURL="10.1.1.90:8081";
        public String subjectValueName;
    }

    ;

    static Config config;


    public static void main(String[] args) {

        SparkSession spark;

        spark = SparkSession.builder()
//                    .master("local")
                .appName("Welder")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> ds = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.bootstrap_servers)
                .option("subscribe", config.topic )
                .option("startingOffsets", "earliest") // From starting
                .load();

        ds.printSchema();

        String jsonFormatSchema = null;
        try {
            jsonFormatSchema = getLastestSchema(config.topic);
            Dataset<Row> personDS= ds.select(from_avro(col("value"), jsonFormatSchema).as("person")).select("person.*");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }



    }

 static   private String getLastestSchema(String topic) throws IOException, RestClientException {
        RestService rs = new RestService(config.schemaRegistryURL);
        Schema valueRestResponseSchema = rs.getLatestVersion(config.subjectValueName);
        return valueRestResponseSchema.toString();
    }

}
