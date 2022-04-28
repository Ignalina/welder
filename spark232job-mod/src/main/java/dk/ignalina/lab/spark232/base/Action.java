package dk.ignalina.lab.spark232.base;

import com.google.gson.JsonObject;

public abstract class Action {
    public abstract boolean fire(JsonObject jsonObject);

}
