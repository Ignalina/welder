package dk.ignalina.lab.spark311;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class Utils {

    public static class Config {
        public String topic="topic";
        public String format;
        public String bootstrap_servers="10.1.1.90:9092";

        public String schemaRegistryURL="10.1.1.90:8081";
        public String subjectValueName;
        public String startingOffsets;

        public String checkpointDir;
        public String mode="PERMISSIVE";
        public String failOnDataLoss="false";
    }

    static   public  String getLastestSchema(Config config) throws IOException, RestClientException {
        RestService rs = new RestService(config.schemaRegistryURL);
        Schema valueRestResponseSchema = rs.getLatestVersion(config.subjectValueName);
        return valueRestResponseSchema.getSchema().toString();
    }

    static public  Config parToConfig(String[] args) {
        Config config=new Config();


        config.topic=args[0];
        config.format=args[1];
        config.bootstrap_servers=args[2];
        config.schemaRegistryURL=args[3];;
        config.subjectValueName=args[4];;
        config.checkpointDir=args[5];
        config.startingOffsets=args[6];
        config.mode=args[7];
        return config;
    }

}