CREATE TABLE IF NOT EXISTS  core.pas_rismerkmal (pas_rismerkmal_sid  STRING COMMENT 'Surrgate Key', pas_rismerkmal_id  STRING COMMENT 'Foreign Key', pas_vpvt_id  STRING COMMENT 'Foreign Key', partnerid  INTEGER COMMENT 'id of VpVT (Das Field partnerId identifiziert die versicherte Person, zu der diese Row ein', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbid  INTEGER COMMENT 'the historical version number (logId)', rollenid  INTEGER COMMENT 'id of VpVT (Identifikation einer Partnerrolle in einer Partner-Vertrags-BeziehungConstrain', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', rismerkmalid  INTEGER COMMENT 'id of RisMerkmal (Das Feld risMerkmalId identifiziert das Risikomerkmal, zu dem diese Zeil', partkey  SMALLINT COMMENT 'the partition key', rismerkmaldouble  DECIMAL(16,2) COMMENT 'Gibt den Wert des Risikomerkmals als Double an.', rismerkmalinteger  INTEGER COMMENT 'Gibt den Wert des Risikomerkmals als Integer an.', rismerkmaldate  TIMESTAMP COMMENT 'Gibt den Wert des Risikomerkmals als Date an.', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(partnerid, lvid, rollenid, vtid, rismerkmalid) disable novalidate ) COMMENT 'Risiko-Merkmale sind personenbezogene Merkmale zur Einschaetzung des Risikos.' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');