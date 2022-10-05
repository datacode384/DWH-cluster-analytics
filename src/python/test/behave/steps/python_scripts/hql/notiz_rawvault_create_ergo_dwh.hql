CREATE TABLE IF NOT EXISTS ${hiveconf:database}.${hiveconf:table_name}  (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', kontextid  INTEGER COMMENT 'id of Notiz (Eindeutige Rolle der Notiz in dem zugeordneten Vertrag.)', textinhalt  STRING COMMENT 'Freitext innerhalb einer Notiz', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'In der Table notiz koennen beliebige Freitexte zu einem Vertrag abgelegt werden.' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;