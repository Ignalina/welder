package dk.ignalina.lab.spark301;

import com.google.gson.JsonObject;
import dk.ignalina.lab.spark301.base.Action;
import dk.ignalina.lab.spark301.base.EventSparkStreamingKafka;
import dk.ignalina.lab.spark301.base.Utils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class KafkaEventDrivenSparkJob2 extends EventSparkStreamingKafka {


    public static void main(String... args) {

        Action actionImpl = new Action() {
            @Override
            public boolean fire(JsonObject jsonObject, SparkSession spark) {
                System.out.println("and action !!!!!!!!!!!!!!!1");

                System.out.println(jsonObject.toString());
                String filename = jsonObject.get("body").getAsJsonObject().get("name").getAsString();
                System.out.println("Fick ett event med S3 fil och body.name=" + filename);
                Dataset<Row> parquetFileDF = spark.read().parquet(filename);
                parquetFileDF.printSchema();
                return true;
            }
        };
        Utils.Config config = new Utils.Config(args);
        SparkConf conf = EventSparkStreamingKafka.CreateSparkConf("v20220610 spark 3.0.1 streaming event job ");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(config.msIntervall));

        JavaInputDStream<ConsumerRecord<String, GenericRecord>> stream = EventSparkStreamingKafka.getStreamingContext(config, ssc,actionImpl);

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}
