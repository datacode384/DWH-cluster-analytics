CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}  (sksga_sid  STRING COMMENT 'Surrgate Key', sksga_id  STRING COMMENT 'Foreign Key Key table', bearbnw_sid  STRING COMMENT 'Foreign Key', lv_sid  STRING COMMENT 'Foreign Key', vb_sid  STRING COMMENT 'Foreign Key', vt_sid  STRING COMMENT 'Foreign Key', bearbnw_id  STRING COMMENT 'Foreign Key', lv_id  STRING COMMENT 'Foreign Key', vb_id  STRING COMMENT 'Foreign Key', vt_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', vbid  SMALLINT COMMENT 'id of Vb (generically derived from Vb)', sgaerr  DECIMAL(2,2) COMMENT 'Erreichter Stand des Schlussueberschussguthabens.', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', sgazutbed  DECIMAL(2,2) COMMENT 'Bedingte Zuteilung zur Erhoehung der Schlussgewinnanwartschaft', partkey  SMALLINT COMMENT 'the partition key', konspartid  INTEGER COMMENT 'id of SkSga (Das Field konsPartID identifiziert den Konsortialpartner.)', diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(bearbidabg, konspartid, lvid, vbid, vtid) disable novalidate ) COMMENT 'Sammelkontostand fuer die Schlussueberschussanwartschaft Kontostand, der den erreichten Stand des Schlussueberschussguthabens ausweist.Historie-streng: Attribute, die zur strengen Historienfuehrung am Vertrag benoetigt werdenSK-Schlussueberschuss-Anwart:' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;