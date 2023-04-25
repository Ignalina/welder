/*
 * MIT No Attribution
 *
 * Copyright 2023 Rickard Ernst Björn Lundin (rickard@ignalina.dk)
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

package dk.ignalina.lab.spark332;


import dk.ignalina.lab.spark332.base.Utils;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class IcebergToSolr {


    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
       SparkSession spark=Utils.createSpark(args,"IcebergToSolr");

        Dataset<Row> df = spark.readStream()
                .format("iceberg")
                .option("stream-from-timestamp", Long.toString(0))
                .load("nessie.piwik");

        Dataset<Row> df2= df.select("session_entry_title", "session_second_title", "is_bounce","is_exit","is_entry","event_type__label","event_url");

        df2
                .writeStream()
                .outputMode("append")
                .foreach(Utils.saveToSolr())
                .start()
                .awaitTermination();

    }




}


