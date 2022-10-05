CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}  (sres_sid  STRING COMMENT 'Surrgate Key', sres_id  STRING COMMENT 'Foreign Key Key table', lv_sid  STRING COMMENT 'Foreign Key', lv_id  STRING COMMENT 'Foreign Key', wirkdat  TIMESTAMP COMMENT 'Schadensanmeldung', schadendat  TIMESTAMP COMMENT 'Schadensanmeldung', meldedat  TIMESTAMP COMMENT 'meldeDat', lvid  STRING COMMENT 'id of Sres (Das Field lvID identifiziert den Vertrag, zu dem diese Row gehoert.)', pdid  STRING COMMENT 'pdID', tfid  STRING COMMENT 'tfID', varianteid  STRING COMMENT 'Attribut zur eindeutigen Identifizierung einer Tarifvariante.', sex  INTEGER COMMENT 'sex', zustand  INTEGER COMMENT 'In diesem Attribut wird der aktuelle Status des Vertragsbausteins verschluesselt.', kzkoll  SMALLINT COMMENT 'kzKoll', mandantid  INTEGER COMMENT 'mandantID', waehrungid  INTEGER COMMENT 'waehrungID', konsortid  INTEGER COMMENT 'konsortID', kzschadenresbuz  SMALLINT COMMENT 'kzschadenresBuz', kzschadenrestod  SMALLINT COMMENT 'kzschadenresTod', schrittid  INTEGER COMMENT 'schrittid', lstver  DECIMAL(2,2) COMMENT 'lstver', leistbon  DECIMAL(2,2) COMMENT 'leistbon', lstsgw  DECIMAL(2,2) COMMENT 'lstsgw', lstfvm  DECIMAL(2,2) COMMENT 'lstfvm', lstson  DECIMAL(2,2) COMMENT 'lstson', kzschadenrespflege  INTEGER COMMENT 'Kennzeichen fuer die Pflegerenten-Schadenreserve', kzschadenresdd  INTEGER COMMENT 'Kennzeichen fuer die Dread Disease-Schadenreserve', diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid) disable novalidate ) PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;