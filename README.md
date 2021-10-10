# Welder
This streaming spark job reads avro from kafka and outputs to hive/parquet or iceberg/parquet.
Goals with this is to learn howto read from kafka with multiple partitions/offsets and spread out the work to multiple spark workers.  
  
The name Welder is the opposite from my Shredder , since the welder it makes the data "whole again" (Hopefully)




