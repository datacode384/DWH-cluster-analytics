CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}  (vita_sid  STRING COMMENT 'Surrgate Key', vita_id  STRING COMMENT 'Foreign Key Key table', bearbnw_sid  STRING COMMENT 'Foreign Key', lv_sid  STRING COMMENT 'Foreign Key', vt_sid  STRING COMMENT 'Foreign Key', bearbnw_id  STRING COMMENT 'Foreign Key', lv_id  STRING COMMENT 'Foreign Key', vt_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', tadat  TIMESTAMP COMMENT 'id of VtTa (Termin, zu dem bei einem Vertragsteil zu einem Teilauszahlungstarif eine Teila', taabs  DECIMAL(2,2) COMMENT 'Betrag der vorgesehenen Teilauszahlung zum Termin ""Teilauszahlung-Termin"".', taproz  DECIMAL(6,6) COMMENT 'Angabe der prozentualen Leistung, die bei einem Vertragsteil zu einem Teilauszahlungstarif', partkey  SMALLINT COMMENT 'the partition key', diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(bearbidabg, lvid, tadat, vtid) disable novalidate ) COMMENT 'Spezifische Infos zu Teilauszahlungs-VTs, die nicht im VT enthalten sind.Historie-streng: Attribute, die zur strengen Historienfuehrung am Vertrag benoetigt werdenErgaenzungsteil-Teilauszahlung: In dieser Entitaet werden spezifische Informationen zu Teil' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;