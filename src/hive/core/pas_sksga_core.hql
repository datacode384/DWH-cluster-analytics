CREATE TABLE IF NOT EXISTS  core.pas_sksga (pas_sksga_sid  STRING COMMENT 'Surrgate Key', pas_sksga_id  STRING COMMENT 'Foreign Key', pas_vb_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', vbid  SMALLINT COMMENT 'id of Vb (generically derived from Vb)', sgaerr  DECIMAL(16,2) COMMENT 'Erreichter Stand des Schlussueberschussguthabens.', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', sgazutbed  DECIMAL(16,2) COMMENT 'Bedingte Zuteilung zur Erhoehung der Schlussgewinnanwartschaft', partkey  SMALLINT COMMENT 'the partition key', konspartid  INTEGER COMMENT 'id of SkSga (Das Field konsPartID identifiziert den Konsortialpartner.)', c_korr1  DECIMAL(16,2) COMMENT 'Korrekturbetrag 1', c_korr2  DECIMAL(16,2) COMMENT 'Korrekturbetrag 2', c_begdatsgaerr  TIMESTAMP COMMENT 'Schlussueberschussbeginndatum', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid, vtid, vbid, konspartid) disable novalidate ) COMMENT 'Sammelkontostand fuer die Schlussueberschussanwartschaft Kontostand, der den erreichten Stand des Schlussueberschussguthabens ausweist.Historie-streng: Attribute, die zur strengen Historienfuehrung am Vertrag benoetigt werdenSK-Schlussueberschuss-Anwart:' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');