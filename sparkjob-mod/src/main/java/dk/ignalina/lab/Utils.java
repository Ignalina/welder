package dk.ignalina.lab;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class Utils {

    public static class Config {
        public String topic="topic";
        public String format;
        public String bootstrap_servers="10.1.1.90:9092";
        public String subscribe;
        public String schemaRegistryURL="10.1.1.90:8081";
        public String subjectValueName;
        public String checkpointDir;
    }

    static   public  String getLastestSchema(Config config) throws IOException, RestClientException {
        RestService rs = new RestService(config.schemaRegistryURL);
        Schema valueRestResponseSchema = rs.getLatestVersion(config.subjectValueName);
        return valueRestResponseSchema.toString();
    }

    static public  Config parToConfig(String[] args) {
        Config config=new Config();

        config.topic=args[1];
        config.format=args[2];
        config.bootstrap_servers=args[3];
        config.subscribe=args[4];;
        config.schemaRegistryURL=args[5];;
        config.subjectValueName=args[6];;
        config.checkpointDir=args[7];;

        return null;
    }

}
