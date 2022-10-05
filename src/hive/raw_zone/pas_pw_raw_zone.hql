CREATE TABLE IF NOT EXISTS  raw_zone.pas_pw (productold  STRING, productnew  STRING, tariffold  STRING, tariffnew  STRING, type  STRING, kzwik  INTEGER, fo_belegewik  INTEGER, customer  STRING, insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');