-- usage: hive --hiveconf database=raw_zone --hiveconf table_name=BPMS_L0_DWH_IN_1 -f create_bpms_table_compressed.hql

CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}

(jms_message_id STRING, message_queue STRING, message STRING, jms_timestamp BIGINT, insert_tst BIGINT )

PARTITIONED BY (insert_month SMALLINT, insert_day SMALLINT)

STORED AS parquet 

TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
