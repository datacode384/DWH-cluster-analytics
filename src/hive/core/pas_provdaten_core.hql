CREATE TABLE IF NOT EXISTS  core.pas_provdaten (pas_provdaten_sid  STRING COMMENT 'Surrgate Key', pas_lv_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', abagenturid  STRING COMMENT 'Nummer der Abschluss-Agentur', beagenturid  STRING COMMENT 'Nummer der Bestands-Agentur', miagenturid  STRING COMMENT 'Nummer der mitwirkenden Agentur', kzmanprov  INTEGER COMMENT 'Kennzeichen, ob die Provision manuell zu vergeben oder zu veraendern ist', kzvertraktion  INTEGER COMMENT 'Kennzeichen fuer Vertriebsaktionen.', kzwettbew  INTEGER COMMENT 'Kennzeichen fuer Vertriebswettbewerbe', bewsfakt  DECIMAL(16,8) COMMENT 'Bewertungsfaktor bei der Provisionierung', provkuerz  DECIMAL(16,8) COMMENT 'Faktor zur Kuerzung der Provision', kzprovdyn  INTEGER COMMENT 'Gibt an, wie im Falle eines Vermittler-Wechsels die Provision aus Dynamik aufgeteilt wird.', kzstores  INTEGER COMMENT 'Gibt Besonderheiten bei der Behandlung der Stornoreserve an.', kzmarket  INTEGER COMMENT 'Enthaelt eine Kennzeichnung des Marketings.', vertriebsweg  INTEGER COMMENT 'Vertriebsweg', maxzinsmod  DECIMAL(16,8) COMMENT 'maximaler Zinssatz fuer die Modellrechnung, vertriebswegabhaengig', zahlwegqual  INTEGER COMMENT 'Naehere Qualifikation des Zahlwegs', provart  INTEGER COMMENT 'Art des Provisionssatzes (Schluesselverzeichnis)1 - Abschlussprovision2 - Bestandsprovisio', kzberatprot  INTEGER COMMENT 'Kennzeichen Beratungsprotokoll', kzbako  INTEGER COMMENT 'Kennzeichen BAKO', kzlabel  INTEGER COMMENT 'Auspraegung: WL - Wuertt. Leben; KLV - Karlsruher Leben; Sonst - sonstige Altvertraege.', bearbstdort  INTEGER COMMENT 'Bearbeitungsstandort', beratprotpflicht  INTEGER COMMENT 'Beratungsprotokoll', partkey  SMALLINT COMMENT 'the partition key', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid) disable novalidate ) COMMENT 'Fuehrung historisch relevanter Provisionsdaten am Vertrag' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');