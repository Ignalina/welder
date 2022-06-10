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

package dk.ignalina.lab.spark301.base;

//import com.databricks.spark.avro.SchemaConverters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

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
        public int msIntervall = 2000;
        public Object groupId="groupid";
        public Object autoOffsetReset="latest";
        public String s3AccessKey;
        public String s3SecretKey;
        public String s3EndPoint;
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
            s3AccessKey=args[10];
            s3SecretKey=args[11];
            s3EndPoint=args[12];
        }

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


}
