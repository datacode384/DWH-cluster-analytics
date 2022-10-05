-- need all the keys used for hashing lv_id ex: https://github.ibmgcloud.net/krishna-damarla1/DWH-Cluster-Analytics/blob/master/src/remote/python/lv_wrapper.py#L37

CREATE TABLE IF NOT EXISTS ${hiveconf:database}.key_${hiveconf:table_name} (
-- primary key
lv_id STRING,
-- secondary key (sk)
lvid VARCHAR(20),
tenant_name STRING,
-- technical fields 
insert_tst TIMESTAMP,
process_id INTEGER,
source_system STRING,
) STORED AS PARQUET;
