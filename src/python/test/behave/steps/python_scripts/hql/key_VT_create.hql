CREATE TABLE IF NOT EXISTS ${hiveconf:database}.key_${hiveconf:table_name} (
hvgruppeid smallint,
insert_tst TIMESTAMP,
lvid VARCHAR(20)  ,
process_id INTEGER,
source_system STRING,
tenant_name STRING,
vt_id STRING,
vtid smallint
) STORED AS PARQUET;
