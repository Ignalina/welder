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
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;


public class CsvToIceberg {


    public static void main(String[] args) {
        Utils.Config config = new Utils.Config(args);
        SparkSession.Builder sparkBuilder=  Utils.decorateWithS3(config);
        //for a local spark instance
        SparkConf conf = new SparkConf()
                .setAppName("CsvToIceberg")
                .setMaster(config.master)
                .set("spark.submit.deployMode", "cluster")
                .set("spark.jars.packages", config.packages)
                .set("spark.hadoop.nessie.url", config.url)
                .set("spark.hadoop.nessie.ref", config.branch)
                .set("spark.hadoop.nessie.authentication.type", config.authType)
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.projectnessie.spark.extensions.NessieSpark32SessionExtensions")
                .set("spark.delta.logStore.class", "org.projectnessie.deltalake.NessieLogStore")
                .set("spark.delta.logFileHandler.class", "org.projectnessie.deltalake.NessieLogFileMetaParser");

        JavaStreamingContext ssc = new JavaStreamingContext(new SparkConf().setAppName("v20220620 spark 3.2.0 Streaming /event driven spark job"), new Duration(config.msIntervall));
        SparkSession spark = sparkBuilder
                .master(config.master)
                .config(conf)
                .getOrCreate();


    }
}
