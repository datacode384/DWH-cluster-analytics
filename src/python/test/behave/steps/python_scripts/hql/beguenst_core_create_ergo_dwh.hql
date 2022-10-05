CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}  (beguenst_sid  STRING COMMENT 'Surrgate Key', beguenst_id  STRING COMMENT 'Foreign Key Key table', bearbnw_sid  STRING COMMENT 'Foreign Key', lv_sid  STRING COMMENT 'Foreign Key', bearbnw_id  STRING COMMENT 'Foreign Key', lv_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', beguenstid  SMALLINT COMMENT 'id of Beguenst (generically derived from Beguenst)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bezrechtid  INTEGER COMMENT 'Kennzeichen zur Beschreibung des Bezugsrechtstyps (Standard, ...).', bezartid  INTEGER COMMENT 'Kennzeichen fuer die Zuordnung des Bezugsrechts zu Risikoarten (Erleben, Tod, ...).', widerrufid  INTEGER COMMENT 'Kennzeichen fuer die Qualitaet eines Bezugsrechts (widerruflich, unwiderruflich, ...)', inhalt  STRING COMMENT 'Dieses Attribut beinhaltet einen Freitext variabler Laenge.', sex  INTEGER COMMENT 'Identifizierender Schluessel fuer das Geschlecht einer natuerlichen Person.', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', rangbezugsrecht  INTEGER COMMENT 'Rangfolge des personenbezogenen Bezugsrechts; eindeutig innerhalb eines Vertrages.', partnerid  INTEGER COMMENT 'Das Field partnerId identifiziert denPartner, zu dem diese Row eine Aussage macht.', anteil  DECIMAL(6,6) COMMENT 'Anteil in % des jeweiligen Bezugsrechts.', diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(bearbidabg, beguenstid, lvid) disable novalidate ) COMMENT 'Die Rows in table beguenst repraesentieren beguenstigte Personen eines Vertrages. Es wird festgelegt, wer fuer die jeweilige Leistungsart der Beguenstigte ist.Schluesselwerte legen fest, wer fuer die jeweilige Leistungsart (Tod, Erleben, ...) der Beguens' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;