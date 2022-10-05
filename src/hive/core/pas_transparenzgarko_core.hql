CREATE TABLE IF NOT EXISTS  core.pas_transparenzgarko (pas_transparenzgarko_sid  STRING COMMENT 'Surrgate Key', pas_transparenzgarko_id  STRING COMMENT 'Foreign Key', pas_vt_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', partkey  SMALLINT COMMENT 'the partition key', varianteid  STRING COMMENT 'id of TransparenzGarKo (Attribut zur eindeutigen Identifizierung einer Tarifvariante (wie ', lfdnr  INTEGER COMMENT 'id of TransparenzGarKo (Laufende Nummer des Datensatzes innerhalb der Satzmenge eines VTs ', garbetr  DECIMAL(15,2) COMMENT 'Absolutbetrag der Garantiekosten waehrend dieses Zeitabschnittes.', garprz  DECIMAL(10,8) COMMENT 'Relative Hoehe der Garantiekosten im Zeitabschnitt. (relativ zum Zahlbeitrag)', begdat  TIMESTAMP COMMENT 'Beginn des Zeitabschnitts', enddat  TIMESTAMP COMMENT 'Ende des Zeitabschnitts (bzw. erster Tag der nicht dazu gehoert)', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid, vtid, varianteid, lfdnr) disable novalidate ) COMMENT 'Diese Tabelle wird parallel zu den Transparenzdaten mit dem Verlauf der Garantiekosten gefuellt.Nach dem Beginn einer Beitragspause kann der Verlauf der Garantiekosten so komplex werde dass er in den Transparenzdaten oder in Form einzelner Attribute in s' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');