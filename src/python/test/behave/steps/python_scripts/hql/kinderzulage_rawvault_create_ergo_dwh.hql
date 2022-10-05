CREATE TABLE IF NOT EXISTS ${hiveconf:database}.${hiveconf:table_name}  (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', hvgruppeid  SMALLINT COMMENT 'id of HvGruppe (generically derived from HvGruppe)', bearbid  INTEGER COMMENT 'the historical version number (logId)', kindid  SMALLINT COMMENT 'id of Kinderzulage (Identifizierung des Kindes)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', geburtsjahr  SMALLINT COMMENT 'Geburtsjahr des Kindes', jahrletzezulage  SMALLINT COMMENT 'Jahr der letzten Zulage', zuordnung  INTEGER COMMENT 'Kennzeichnung, ob die Kinderzulage dem Vertrag des VN oder einem Vertrag des Ehegatten gut', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Daten zur Riester-Kinderzulage' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;