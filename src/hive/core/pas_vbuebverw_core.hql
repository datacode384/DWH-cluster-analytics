CREATE TABLE IF NOT EXISTS  core.pas_vbuebverw (pas_vbuebverw_sid  STRING COMMENT 'Surrgate Key', pas_vb_id  STRING COMMENT 'Foreign Key', pas_vbuebverw_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbid  INTEGER COMMENT 'the historical version number (logId)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', vbid  SMALLINT COMMENT 'id of Vb (generically derived from Vb)', vsys  INTEGER COMMENT 'id of VbUebVerw (Systematik fuer die Ueberschuss - Verwendung: ratierlich oder Stichtag)', partkey  SMALLINT COMMENT 'the partition key', verans  DECIMAL(16,2) COMMENT 'Erreichter Stand des Ansammlungsguthabens.', zutverans  DECIMAL(16,2) COMMENT 'Ausweisung unterjaehriger Zuteilungen ins Ansammlungsguthaben (zur Vermeidung von Zinseszi', zutkorvs  DECIMAL(16,2) COMMENT 'Vorschuessig zugeteilte, aber noch nicht verwendete Ueberschussanteile.', zutkorns  DECIMAL(16,2) COMMENT 'Nachschuessig zugeteilte, aber noch nicht verwendete Ueberschussanteile. Nachschuessige Ue', zutkorvs_bewres  DECIMAL(16,2) COMMENT 'Vorschuessig zugeteilte, aber noch nicht verwendete ueberschussanteile aus BWR', zutkorns_bewres  DECIMAL(16,2) COMMENT 'Nachschuessig zugeteilte, aber noch nicht verwendete ueberschussanteile aus BWR', c_beiverrns  DECIMAL(16,2) COMMENT 'Nachschuessig zugeteilte Ueberschuesse fuer die Verrechnung mit Beitraegen des folgenden V', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid, vtid, vbid, vsys) disable novalidate ) COMMENT 'Die Tabelle vbUebVerw wird bei vor- oder nachschuessigen UeberschussVerwendungen mit Ueberschusswerten auf Bausteinebene gefuellt und dient als Basis fuer die bisherige Tabelle skUebVerw auf Vertragsteilebene.' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');