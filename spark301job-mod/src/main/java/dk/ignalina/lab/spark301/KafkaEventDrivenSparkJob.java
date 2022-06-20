package dk.ignalina.lab.spark301;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dk.ignalina.lab.spark301.base.Utils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class KafkaEventDrivenSparkJob {
    static JsonParser parser = new JsonParser();


    public static void main(String... args) {

        Utils.Config config = new Utils.Config(args);
        JavaStreamingContext ssc = new JavaStreamingContext(new SparkConf().setAppName("v20220612 spark 3.0.1 Streaming /event driven spark job"), new Duration(config.msIntervall));
        SparkSession spark=Utils.createS3SparkSession(config);
        JavaInputDStream<ConsumerRecord<String, GenericRecord>> stream = Utils.createStream(config,ssc);


        stream.foreachRDD(rdd -> {
                    JavaRDD<String> filenames = rdd.map(record -> extractFileName(record)); // VARNING/TODO: List needs to be sorted on date for correct Delta ingest order.
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
    public static String extractFileName(ConsumerRecord<String, GenericRecord> record) {

        String message = ""+record.value();
        JsonObject jo = null;
        try {
            jo = JsonParser.parseString(message).getAsJsonObject();
        } catch (IllegalStateException ei) {
            String res="JSON CONTAINED NO PARSABLE FILENAME";
            System.out.println(res);
            return res;
        }
        String filename=jo.get("body").getAsJsonObject().get("name").getAsString();
        System.out.println("Filename extraced from JSON=" + filename);

        return filename;
    }

}
