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

package dk.ignalina.lab.spark320;

import org.apache.spark.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;



public class AppDeltaStreamingKafka {


    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("appName")
                .setMaster("spark://10.1.1.193:7077")
                .set("spark.submit.deployMode", "cluster")
                .set("spark.jars.packages", "org.projectnessie:nessie-deltalake:0.16.0")
                .set("spark.hadoop.nessie.url", "10.1.1.192:19120")
                .set("spark.hadoop.nessie.ref", "branch")
                .set("spark.hadoop.nessie.authentication.type","NONE")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.delta.logStore.class", "org.projectnessie.deltalake.NessieLogStore")
                .set("spark.delta.logFileHandler.class", "org.projectnessie.deltalake.NessieLogFileMetaParser");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

        SparkSession spark = SparkSession.builder()
//                .master("local[2]")
                .config(conf)
                .getOrCreate();
    }


}
