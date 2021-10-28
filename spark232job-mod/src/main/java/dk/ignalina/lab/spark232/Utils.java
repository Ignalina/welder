package dk.ignalina.lab.spark232;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Utils {

    public static class Config {
        public String topic="topic";
        public String format;
        public String bootstrap_servers="10.1.1.90:9092";

        public String schemaRegistryURL="10.1.1.90:8081";
        public String subjectValueName;
        public String startingOffsets;

        public String checkpointDir;
        public String mode="PERMISSIVE";
        public String failOnDataLoss="false";
        public String master="local[2]";
    }

    static   public  String getLastestSchema(Config config) throws IOException, RestClientException {
            return getLastestSchema(config.schemaRegistryURL, config.subjectValueName);
    }

    static   public  String getLastestSchema(String schemaRegistryURL,String subjectValueName) throws IOException, RestClientException {
        RestService rs = new RestService(schemaRegistryURL);
        Schema valueRestResponseSchema = rs.getLatestVersion(subjectValueName);
        return valueRestResponseSchema.getSchema().toString();
    }

    static public  Config parToConfig(String[] args) {
        Config config=new Config();


        config.topic=args[0];
        config.format=args[1];
        config.bootstrap_servers=args[2];
        config.schemaRegistryURL=args[3];;
        config.subjectValueName=args[4];;
        config.checkpointDir=args[5];
        config.startingOffsets=args[6];
        config.mode=args[7];
        return config;
    }

    static public void createHiveTable(Dataset<Row> df, String tableName,SparkSession spark) {
        String tmpTableName="my_temp"+tableName;

        df.createOrReplaceTempView(tmpTableName);
        spark.sql("drop table if exists "+ tableName);
        spark.sql("create table "+tableName +" as select * from "+tmpTableName);

    }
}
