CREATE TABLE IF NOT EXISTS ${hiveconf:database}.${hiveconf:table_name}  (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbid  INTEGER COMMENT 'the historical version number (logId)', infotyp  INTEGER COMMENT 'id of Transparenz (Art einer Information)', infobetrag  DECIMAL(5,5) COMMENT 'Information in Form einer Zahl', infobezug  INTEGER COMMENT 'Bezugsgroesse, auf die sich die ermittelten Werte zu dem Typ beziehen', varianteid  STRING COMMENT 'id of Transparenz (Attribut zur eindeutigen Identifizierung einer Tarifvariante (wie in sb', gueab  TIMESTAMP COMMENT 'Gueltig ab, Beginntermin der Zuordnung, Zuordnung ist zum Beginntermin und ab dem Beginnte', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Eine Zeile in der Tabelle ""transparenz"" repraesentiert eine Information, die an den Versicherungsnehmer weitergegeben werden muss.' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;