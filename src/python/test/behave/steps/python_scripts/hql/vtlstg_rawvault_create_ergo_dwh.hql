CREATE TABLE IF NOT EXISTS ${hiveconf:database}.${hiveconf:table_name}  (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', lstgtyp  INTEGER COMMENT 'id of VtLstg (Leistungsart)', partkey  SMALLINT COMMENT 'the partition key', bearbid  INTEGER COMMENT 'the historical version number (logId)', lstgwertakt  DECIMAL(4,4) COMMENT 'Aktueller Leistungswert', lstgwertrel  DECIMAL(8,8) COMMENT 'Relativer Leistungswert', vorgtyp  INTEGER COMMENT 'Steuert, ob die Leistung relativ oder absolut vorgegeben wird', lstgprofil  INTEGER COMMENT 'Leistungsprofil fuer die aktuelle Leistungsart', lstgoption  INTEGER COMMENT 'Das Attribut steuert, wie die Leistungen beim ausserplanmaessigen Leistungsuebergang angep', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Speicherung von Leistungen und Leistungsvorgabe je Leistungsart am Vertragsteil' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;