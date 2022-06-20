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
import dk.ignalina.lab.spark320.base.Utils;




public class AppDeltaStreamingKafka {


    private static String authType;

    public static void main(String[] args) {

        Utils.Config config = new Utils.Config(args);
//        JavaStreamingContext ssc = new JavaStreamingContext(new SparkConf().setAppName("v20220612 spark 3.0.1 Streaming /event driven spark job"), new Duration(config.msIntervall));
//        SparkSession spark=Utils.createS3SparkSession(config);
//        JavaInputDStream<ConsumerRecord<String, GenericRecord>> stream = Utils.createStream(config,ssc);
        Utils.decorateS3SparkSession(config);
        //for a local spark instance
        SparkConf conf = new SparkConf()
                .setAppName("appName")
                .setMaster("spark://10.1.1.193:7077")
                .set("spark.submit.deployMode", "cluster")
                .set("spark.jars.packages", "org.projectnessie:nessie-deltalake:0.30.0,org.projectnessie:nessie-spark-3.2-extensions:0.30.0")
                .set("spark.hadoop.nessie.url", config.url)
                .set("spark.hadoop.nessie.ref", config.branch)
                .set("spark.hadoop.nessie.authentication.type", config.authType)
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.projectnessie.spark.extensions.NessieSpark32SessionExtensions")
                .set("spark.delta.logStore.class", "org.projectnessie.deltalake.NessieLogStore")
                .set("spark.delta.logFileHandler.class", "org.projectnessie.deltalake.NessieLogFileMetaParser");


        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));

//        spark = SparkSession.builder()
//                .master("local[2]")
//                .config(conf)
//                .getOrCreate();

        SparkSession spark = SparkSession.builder()
                .master(config.master)
                .config(conf)
                .getOrCreate();
    }


}
