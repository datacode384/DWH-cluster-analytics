CREATE TABLE IF NOT EXISTS  raw_zone.pas_transparenzgarko (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', partkey  SMALLINT COMMENT 'the partition key', varianteid  STRING COMMENT 'id of TransparenzGarKo (Attribut zur eindeutigen Identifizierung einer Tarifvariante (wie ', lfdnr  INTEGER COMMENT 'id of TransparenzGarKo (Laufende Nummer des Datensatzes innerhalb der Satzmenge eines VTs ', garbetr  DECIMAL(15,2) COMMENT 'Absolutbetrag der Garantiekosten waehrend dieses Zeitabschnittes.', garprz  DECIMAL(10,8) COMMENT 'Relative Hoehe der Garantiekosten im Zeitabschnitt. (relativ zum Zahlbeitrag)', begdat  TIMESTAMP COMMENT 'Beginn des Zeitabschnitts', enddat  TIMESTAMP COMMENT 'Ende des Zeitabschnitts (bzw. erster Tag der nicht dazu gehoert)', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Diese Tabelle wird parallel zu den Transparenzdaten mit dem Verlauf der Garantiekosten gefuellt.Nach dem Beginn einer Beitragspause kann der Verlauf der Garantiekosten so komplex werde dass er in den Transparenzdaten oder in Form einzelner Attribute in s' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');