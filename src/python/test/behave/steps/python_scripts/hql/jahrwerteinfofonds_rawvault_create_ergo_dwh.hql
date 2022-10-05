CREATE TABLE IF NOT EXISTS ${hiveconf:database}.${hiveconf:table_name}  (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', partkey  SMALLINT COMMENT 'the partition key', fondsid  INTEGER COMMENT 'id of JahrWerteInfoFonds (Interne Kennummer eines Fonds)', monat  SMALLINT COMMENT 'id of JahrWerteInfoFonds (Angabe eines Monats)', jahr  SMALLINT COMMENT 'Angabe einer Jahreszahl (JJJJ)', fvm  DECIMAL(2,2) COMMENT 'Fondsvermoegen', fondskickback  DECIMAL(2,2) COMMENT 'Fondskickbacks im Rahmen der laufenden (Kosten-)Ueberschussbeteiligung', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Fondsspezifische Werte fuer die jaehrliche Information, die im Laufe des aktuellen Jahres gesammelt werden.' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;