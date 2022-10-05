CREATE TABLE IF NOT EXISTS  core.pas_pdtf (pdtf_sid  STRING COMMENT 'Surrgate Key', pdtf_id  STRING COMMENT 'Foreign Key', productid  STRING, tariffid  STRING, tpoblfak  STRING, customer  STRING, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(productid, tariffid) disable novalidate ) PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');