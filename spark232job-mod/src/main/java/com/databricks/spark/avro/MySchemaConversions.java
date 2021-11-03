package com.databricks.spark.avro;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

public class MySchemaConversions {
    public DataType createConverterToSQL(Schema avroSchema, DataType sparkSchema ) {
        return  SchemaConverters.toSqlType(avroSchema).dataType();

    }

//    public Function1<GenericRecord, Row> createConverterToSQL(Schema avroSchema, DataType sparkSchema ) {
    //:(GenericRecord) =>Row =
//                SchemaConverters.toSqlType(avroSchema);

                // createConverterToSQL(avroSchema, sparkSchema).instanceof[(GenericRecord) = > Row]
//        return null;
//    }
}



