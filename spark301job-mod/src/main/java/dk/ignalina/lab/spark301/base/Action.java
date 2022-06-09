package dk.ignalina.lab.spark301.base;

import com.google.gson.JsonObject;
import org.apache.spark.sql.SparkSession;

public abstract class Action {
    public abstract boolean fire(JsonObject jsonObject, SparkSession spark);

}
