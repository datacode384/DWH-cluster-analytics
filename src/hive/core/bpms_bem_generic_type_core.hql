CREATE TABLE IF NOT EXISTS  core.bpms_bem_generic_type (bpms_bem_generic_type_sid  STRING COMMENT 'Surrgate Key', bem_generic_id  STRING COMMENT 'Foreign Key', bpms_bem_business_event_id  STRING COMMENT 'Foreign Key', bpms_bem_complaint_id  STRING COMMENT 'Foreign Key', bpms_bem_complaint_relation_id  STRING COMMENT 'Foreign Key', bpms_bem_e2e_process_id  STRING COMMENT 'Foreign Key', bpms_bem_e2e_process_timeslot_id  STRING COMMENT 'Foreign Key', bpms_bem_external_key_id  STRING COMMENT 'Foreign Key', bpms_bem_general_process_step_id  STRING COMMENT 'Foreign Key', bpms_bem_package_entry_id  STRING COMMENT 'Foreign Key', bpms_bem_process_instance_id  STRING COMMENT 'Foreign Key', bpms_bem_processing_goal_id  STRING COMMENT 'Foreign Key', bpms_document_id  STRING COMMENT 'Foreign Key', id  INTEGER, type  STRING, status  SMALLINT, value  STRING, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(id) disable novalidate ) PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');