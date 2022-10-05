CREATE TABLE IF NOT EXISTS  raw_zone.bpms_bem_output_document (general_document_id  STRING, generation_timestamp  TIMESTAMP, print_timestamp  TIMESTAMP, receiver_id  STRING, send_timestamp  TIMESTAMP, business_event_id  STRING, e2e_timeslot_id  STRING, output_chanel_id  INTEGER, output_document_status_id  INTEGER, output_document_type_id  INTEGER, insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');