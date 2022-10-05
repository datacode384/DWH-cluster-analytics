CREATE TABLE IF NOT EXISTS  core.pas_bearbnw (pas_bearbnw_sid  STRING COMMENT 'Surrgate Key', pas_lv_id  STRING COMMENT 'Foreign Key', lvid  STRING, bearbid  INTEGER, schrittid  INTEGER, bearbeiterid  STRING, bearbdat  TIMESTAMP, wirkbeginn  TIMESTAMP, feinbeginn  SMALLINT, wirkende  TIMESTAMP, feinende  SMALLINT, bearbidsto  INTEGER, bearbdatsto  TIMESTAMP, jobid  STRING, bearbgrdid  INTEGER, meldedat  TIMESTAMP, wterm  TIMESTAMP, bearbzeit  STRING, schwebeid_4augen  INTEGER, bearbeiterid_4a  STRING, partkey  SMALLINT, businessprocessid  INTEGER, kbfauftragsid  STRING, bearbkategorie  STRING, kzilisrelevant  SMALLINT, bearbresult  SMALLINT, bearbidabg  INTEGER, c_aussteuerung  INTEGER, tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid) disable novalidate ) PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');