CREATE TABLE IF NOT EXISTS  raw_zone.bpms_bem_generic_type (id  INTEGER, type  STRING, status  SMALLINT, value  STRING, insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');