CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}  (versausglentn_sid  STRING COMMENT 'Surrgate Key', versausglentn_id  STRING COMMENT 'Foreign Key Key table', bearbnw_sid  STRING COMMENT 'Foreign Key', hvgruppe_sid  STRING COMMENT 'Foreign Key', lv_sid  STRING COMMENT 'Foreign Key', bearbnw_id  STRING COMMENT 'Foreign Key', hvgruppe_id  STRING COMMENT 'Foreign Key', lv_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', hvgruppeid  SMALLINT COMMENT 'id of HvGruppe (generically derived from HvGruppe)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidref  INTEGER COMMENT 'Das Field bearbIdRef identifiziert zusammen mit dem Field lvId den Vertragsstand, von dem ', entn_dk  DECIMAL(2,2) COMMENT 'Entnahmebetrag Deckungskapital', entn_fvm  DECIMAL(2,2) COMMENT 'Entnahmebetrag Fondsvermoegen', entn_lue  DECIMAL(2,2) COMMENT 'Entnahmebetrag Lfd. Ueberschuesse', entn_sue  DECIMAL(2,2) COMMENT 'Entnahmebetrag Schlussueberschuss', entn_lstuebbr  DECIMAL(2,2) COMMENT 'Entnahmebetrag Leistung aus Bewertungsreserven', entn_ausglwert  DECIMAL(2,2) COMMENT 'Entnahmebetrag Ausgleichswert', entn_kosten  DECIMAL(2,2) COMMENT 'Entnahmebetrag Kosten', entn_datum  TIMESTAMP COMMENT 'Datum der Entnahme', basisuebbr  DECIMAL(2,2) COMMENT 'Bewegungen in den Basisgroessen fuer Bewertungsreserve', basissue  DECIMAL(2,2) COMMENT 'Bewegungen in den Basisgroessen fuer Schlussueberschuss', entn_ausglwertvorg  DECIMAL(2,2) COMMENT 'vorgegebener Entnahmebetrag Ausgleichswert vor Umbewertung', beschlussdat  TIMESTAMP COMMENT 'Datum des Beschlusses des Familiengerichts zu einem Versorgungsausgleich. Ist fuer Riester', kzversausglueb  INTEGER COMMENT 'Kennzeichen, ob im Rahmen des Versorgungsausgleichs eine Uebertragung der Bezugsgroessen a', prozueb  DECIMAL(8,8) COMMENT 'Der uebertragene Prozentsatz bei VA Riester', zins  DECIMAL(4,4) COMMENT 'Zinssatz, mit dem der vorgegebene Ausgleichswert zwischen Eheende und Rechtskraft des Besc', zinsbis  TIMESTAMP COMMENT 'Termin bis zu dem der vorgegebene Ausgleichswert ab dem Eheende verzinst wird. Dies ist in', kapitalverzehrsfaktor  DECIMAL(8,8) COMMENT 'Der Kapitalverzehrsfaktor basiert auf einem Barwertvergleich. Der Wert 1 - Kapitalverzehrs', kzumbewertung  INTEGER COMMENT 'Kennzeichen fuer Umbewertung bei Entnahme Versorgungsausgleich', diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(bearbid, hvgruppeid, lvid) disable novalidate ) COMMENT 'Schnittstelle Entnahme Versorgungsausgleich' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;