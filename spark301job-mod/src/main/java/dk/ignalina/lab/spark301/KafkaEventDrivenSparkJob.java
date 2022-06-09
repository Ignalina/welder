package dk.ignalina.lab.spark301;

import com.google.gson.JsonObject;
import dk.ignalina.lab.spark301.base.Action;
import dk.ignalina.lab.spark301.base.EventSparkStreamingKafka;
import dk.ignalina.lab.spark301.base.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class KafkaEventDrivenSparkJob extends EventSparkStreamingKafka {


    public static void main(String... args) {

        actionImpl=new Action() {
            @Override
            public boolean fire(JsonObject jsonObject, SparkSession spark) {
                System.out.println(jsonObject.toString());
                String filename=jsonObject.get("body").getAsJsonObject().get("name").getAsString();
                System.out.println("Fick ett event med S3 fil och body.name="+filename);
                Dataset<Row> parquetFileDF = spark.read().parquet(filename);
                parquetFileDF.printSchema();
                return true;
            }
        };

        Utils.Config config = new Utils.Config(args);
        JavaStreamingContext ssc=EventSparkStreamingKafka.getStreamingContext(config);

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

}
