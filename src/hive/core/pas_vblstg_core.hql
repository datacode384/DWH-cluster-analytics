CREATE TABLE IF NOT EXISTS  core.pas_vblstg (pas_vblstg_sid  STRING COMMENT 'Surrgate Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', pas_vb_id  STRING COMMENT 'Foreign Key', pas_vblstg_id  STRING COMMENT 'Foreign Key', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', vbid  SMALLINT COMMENT 'id of Vb (generically derived from Vb)', lstgtyp  INTEGER COMMENT 'id of VbLstg (Leistungsart )', partkey  SMALLINT COMMENT 'the partition key', bearbid  INTEGER COMMENT 'the historical version number (logId)', lstgwertakt  DECIMAL(18,4) COMMENT 'Aktueller Leistungswert', lstgwertnominal  DECIMAL(18,4) COMMENT 'Nominaler Leistungswert', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid, vtid, vbid, lstgtyp) disable novalidate ) COMMENT 'Speicherung von Leistungen und Leistungsvorgabe je Leistungsart am Vertragsteil' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');