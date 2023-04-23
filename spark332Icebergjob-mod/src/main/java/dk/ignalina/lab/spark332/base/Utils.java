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

package dk.ignalina.lab.spark332.base;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.*;


public class Utils {

    public static class Config {
        public String topic = "topic";
        public String format;
        public String bootstrap_servers = "10.1.1.90:9092";
        public String schemaRegistryURL = "10.1.1.90:8081";
        public String subjectValueName;
        public String startingOffsets;
        public String checkpointDir;
        public String mode = "PERMISSIVE";
        public String failOnDataLoss = "false";
        public String master = "local[2]";
        public int msIntervall = 2000;
        public Object groupId="groupid";
        public Object autoOffsetReset="latest";
        public String s3AccessKey;
        public String s3SecretKey;
        public String s3EndPoint;
        public String branch;
        public String url;
        public String authType;

        public  Config(String[] args) {
            topic = args[0];
            format = args[1];
            bootstrap_servers = args[2];
            schemaRegistryURL = args[3];
            subjectValueName = args[4];
            checkpointDir = args[5];
            startingOffsets = args[6];
            mode = args[7];
            msIntervall = Integer.parseInt(args[8]); ;
            groupId=args[9];
            s3AccessKey=args[10];
            s3SecretKey=args[11];
            s3EndPoint=args[12];
            branch=args[13];
            url=args[14];
            authType=args[15];
        }

    }



    public static SparkConf CreateSparkConf(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName);
        return conf;
    }

    static public SparkSession.Builder decorateWithS3(Utils.Config config) {
        return  SparkSession.builder().master(config.master).
                config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem").
                config("fs.s3a.access.key",config.s3AccessKey).
                config("fs.s3a.secret.key",config.s3SecretKey).
                config("fs.s3a.endpoint", config.s3EndPoint).
                config("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem").
                config("fs.s3a.path.style.access","true");
    }



}
