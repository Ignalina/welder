package dk.ignalina.lab.spark320;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;


public class Utils {

    public static class Config {
        public String topic = "topic";
        public String format;
        public String bootstrap_servers = "10.1.1.90:9092";

        public String schemaRegistryURL = "10.1.1.90:8081";
        public String subjectValueName;
        public String startingOffsets;

        public String checkpointDir;
        public String mode = "PERMISSIVE";
        public String failOnDataLoss = "false";
        public String master = "local[2]";
    }

    static public String getLastestSchema(Config config) throws IOException, RestClientException {
        return getLastestSchema(config.schemaRegistryURL, config.subjectValueName);
    }

    static public String getLastestSchema(String schemaRegistryURL, String subjectValueName) throws IOException, RestClientException {
        RestService rs = new RestService(schemaRegistryURL);
        Schema valueRestResponseSchema = rs.getLatestVersion(subjectValueName);
        return valueRestResponseSchema.getSchema().toString();
    }

    static public Config parToConfig(String[] args) {
        Config config = new Config();


        config.topic = args[0];
        config.format = args[1];
        config.bootstrap_servers = args[2];
        config.schemaRegistryURL = args[3];
        ;
        config.subjectValueName = args[4];
        ;
        config.checkpointDir = args[5];
        config.startingOffsets = args[6];
        config.mode = args[7];
        return config;
    }

    static public void createHiveTable(Dataset<Row> df, String tableName, SparkSession spark) {
        spark.sql("use hiveorg_prod");


        String tables[] = spark.sqlContext().tableNames();
        if (Arrays.asList(tables).contains(tables)) {
            return;
        }

        String tmpTableName = "my_temp" + tableName;

        df.createOrReplaceTempView(tmpTableName);
        spark.sql("drop table if exists " + tableName);
        spark.sql("create table " + tableName + " as select * from " + tmpTableName);

    }

    static public Dataset<Row> createEmptyRDD(SparkSession spark, StructType schema) {
        return spark.createDataFrame(new ArrayList<>(), schema);

    }

    static public StructType avroToSparkSchema(org.apache.avro.Schema avroSchema) {
        return (StructType) SchemaConverters.toSqlType(avroSchema).dataType();
    }

    // Got this from Ram Ghadiyaram
    public static Row avroToRowConverter(GenericRecord avroRecord, StructType structType) {
        if (null == avroRecord) {
            return null;
        }

        Object[] objectArray = new Object[avroRecord.getSchema().getFields().size()];
//        StructType structType = (StructType) SchemaConverters.toSqlType(avroRecord.getSchema()).dataType();
        for (org.apache.avro.Schema.Field field : avroRecord.getSchema().getFields()) {
            if (field.schema().getType().toString().equalsIgnoreCase("STRING") || field.schema().getType().toString().equalsIgnoreCase("ENUM")) {
                objectArray[field.pos()] = "" + avroRecord.get(field.pos());
            } else {
                objectArray[field.pos()] = avroRecord.get(field.pos());
            }
        }

        return new GenericRowWithSchema(objectArray, structType);
    }

}