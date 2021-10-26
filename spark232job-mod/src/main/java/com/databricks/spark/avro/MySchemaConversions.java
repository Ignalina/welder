package com.databricks.spark.avro;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import scala.Function1;

public class MySchemaConversions {
    public Function1<GenericRecord, Row> createConverterToSQL(Schema avroSchema, DataType sparkSchema ) {
    //:(GenericRecord) =>Row =
//                return SchemaConverters.toSqlType() createConverterToSQL(avroSchema, sparkSchema).instanceof[(GenericRecord) = > Row]
        return null;
    }
}



