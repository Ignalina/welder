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

import org.apache.hadoop.shaded.org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import dk.ignalina.lab.spark320.base.Utils;




public class KafkaEventDrivenSparkJob {


    private static String authType;

    public static void main(String[] args) {

        Utils.Config config = new Utils.Config(args);
        SparkSession.Builder sparkBuilder=  Utils.decorateWithS3(config);
        //for a local spark instance
        SparkConf conf = new SparkConf()
                .setAppName("appName")
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

        JavaInputDStream<ConsumerRecord<String, GenericRecord>> stream = Utils.createStream(config,ssc);
        stream.foreachRDD(rdd -> {
            JavaRDD<String> filenames = rdd.map(record -> Utils.extractFileName(record)); // VARNING/TODO: List needs to be sorted on date for correct Delta ingest order.
            filenames.collect().forEach(fileName -> {
                System.out.println("Filename from JSON=" + fileName);
                Dataset<Row> df = spark.read().parquet(fileName);
                df.printSchema();

                // Thats all folks , do something useful with the dataset

            });
        });


        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            // TODO Make sureKAFKA read offset is getting NOT updated OR add to dead letter que !
        }

    }


}
