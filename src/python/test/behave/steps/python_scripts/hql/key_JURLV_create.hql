CREATE TABLE IF NOT EXISTS ${hiveconf:database}.key_${hiveconf:table_name} (
bearbid integer,
insert_tst TIMESTAMP,
jurlv_id STRING,
lvid VARCHAR(20)  ,
process_id INTEGER,
source_system STRING,
tenant_name STRING
) STORED AS PARQUET;
