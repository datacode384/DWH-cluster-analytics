CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}  (notiz_sid  STRING COMMENT 'Surrgate Key', notiz_id  STRING COMMENT 'Foreign Key Key table', bearbnw_sid  STRING COMMENT 'Foreign Key', lv_sid  STRING COMMENT 'Foreign Key', bearbnw_id  STRING COMMENT 'Foreign Key', lv_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', kontextid  INTEGER COMMENT 'id of Notiz (Eindeutige Rolle der Notiz in dem zugeordneten Vertrag.)', textinhalt  STRING COMMENT 'Freitext innerhalb einer Notiz', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(bearbidabg, kontextid, lvid) disable novalidate ) COMMENT 'In der Table notiz koennen beliebige Freitexte zu einem Vertrag abgelegt werden.' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;