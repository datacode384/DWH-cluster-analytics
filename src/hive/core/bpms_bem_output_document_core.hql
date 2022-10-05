CREATE TABLE IF NOT EXISTS  core.bpms_bem_output_document (bpms_bem_output_document_sid  STRING COMMENT 'Surrgate Key', bpms_bem_business_event_id  STRING COMMENT 'Foreign Key', bpms_bem_e2e_process_timeslot_id  STRING COMMENT 'Foreign Key', bpms_document_id  STRING COMMENT 'Foreign Key', general_document_id  STRING, generation_timestamp  TIMESTAMP, print_timestamp  TIMESTAMP, receiver_id  STRING, send_timestamp  TIMESTAMP, business_event_id  STRING, e2e_timeslot_id  STRING, output_chanel_id  INTEGER, output_document_status_id  INTEGER, output_document_type_id  INTEGER, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(general_document_id) disable novalidate ) PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');