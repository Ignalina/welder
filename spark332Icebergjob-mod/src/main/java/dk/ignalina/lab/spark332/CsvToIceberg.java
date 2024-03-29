/*
 * MIT No Attribution
 *
 * Copyright 2023 Rickard Ernst Björn Lundin (rickard@ignalina.dk)
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

package dk.ignalina.lab.spark332;


import dk.ignalina.lab.spark332.base.Utils;
import org.apache.spark.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class CsvToIceberg {


    private static StructType userSchema;

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        Utils.Config config = new Utils.Config(args);
        SparkSession.Builder builder= SparkSession.builder().master(config.master);
        //for a local spark instance
        SparkConf conf = new SparkConf()
                .setAppName("CsvToIceberg")
                .setMaster(config.master)
                .set("spark.submit.deployMode", "cluster")
                .set("spark.sql.streaming.schemaInference","true");

        SparkSession spark = builder
                .master(config.master)
                .config(conf)
                .getOrCreate();

        userSchema=Utils.extractSchema(config.inferSchemaFrom,spark);
        spark.catalog().createTable(config.targetTable,"parquet", userSchema,new HashMap<String,String>());
        Dataset<Row> csvDF = spark
                .readStream()
                .option("sep", ",")
                .option("header","true")
                .schema(userSchema)      // Specify schema of the csv files
                .format(config.fileFormat).load(config.slurpDirectory);


        StreamingQuery sq=csvDF.writeStream()
                .format("iceberg")
                .outputMode("append")
                .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
                .option("path", config.targetTable)
                .option("checkpointLocation", "/tmp")
                .start();



        sq.awaitTermination();
    }
}
