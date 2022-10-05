CREATE TABLE IF NOT EXISTS  raw_zone.bpms_bem_general_process_step (gen_process_step_id  STRING, dtype  STRING, date_insert  TIMESTAMP, date_update  TIMESTAMP, version  INTEGER, process_instance_id  STRING, gen_process_step_type_id  INTEGER, insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');