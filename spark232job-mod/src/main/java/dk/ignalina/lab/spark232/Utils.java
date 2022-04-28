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

import com.databricks.spark.avro.SchemaConverters;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


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
        public int msIntervall = 2000;
        public Object groupId="groupid";
        public Object autoOffsetReset="latest";

        public  Config(String[] args) {
            topic = args[0];
            format = args[1];
            bootstrap_servers = args[2];
            schemaRegistryURL = args[3];
            subjectValueName = args[4];
            checkpointDir = args[5];
            startingOffsets = args[6];
            mode = args[7];
            msIntervall = Integer.parseInt(args[8]); ;
            groupId=args[9];
        }

    }

    static public String getLastestSchema(Config config) throws IOException, RestClientException {
        return getLastestSchema(config.schemaRegistryURL, config.subjectValueName);
    }

    static public String getLastestSchema(String schemaRegistryURL, String subjectValueName) throws IOException, RestClientException {
        RestService rs = new RestService(schemaRegistryURL);
        Schema valueRestResponseSchema = rs.getLatestVersion(subjectValueName);
        return valueRestResponseSchema.getSchema().toString();
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
