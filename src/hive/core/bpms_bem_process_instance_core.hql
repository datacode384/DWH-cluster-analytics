CREATE TABLE IF NOT EXISTS  core.bpms_bem_process_instance (bpms_bem_process_instance_sid  STRING COMMENT 'Surrgate Key', bpms_bem_business_event_id  STRING COMMENT 'Foreign Key', bpms_bem_e2e_process_timeslot_id  STRING COMMENT 'Foreign Key', bpms_bem_process_instance_id  STRING COMMENT 'Foreign Key', process_instance_id  STRING, date_insert  TIMESTAMP, date_update  TIMESTAMP, process_instance_external_id  STRING, process_instance_parent_id  STRING, process_instance_completion_timestamp  TIMESTAMP, process_instance_init_timestamp  TIMESTAMP, version  INTEGER, business_event_id  STRING, e2e_process_timeslot_id  STRING, process_instance_status_id  INTEGER, process_instance_type_id  INTEGER, processing_mode_id  INTEGER, dwh_counter  INTEGER, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(process_instance_id) disable novalidate ) PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');