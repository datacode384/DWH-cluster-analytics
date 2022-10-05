CREATE TABLE IF NOT EXISTS  raw_zone.pas_skschadenres (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbdatsto  TIMESTAMP COMMENT 'Bearbeitungstermin des stornierenden Vorgangs', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', wirkdat  TIMESTAMP COMMENT 'Termin, zu dem eine Aenderung wirksam wird.', feindat  SMALLINT COMMENT 'Feinsteuerungsinformation zum Termin Termin-Wirksamkeit', bearbdat  TIMESTAMP COMMENT 'Termin, an dem das Objekt bearbeitet wurde.', pdid  STRING COMMENT 'Eindeutige Identifikation des Produkts, das dem LV-Vertrag zugeordnet ist.', tfid  STRING COMMENT 'Eindeutiger Identifikator eines LV-Tarifs', sex  INTEGER COMMENT 'Identifizierender Schluessel fuer das Geschlecht einer natuerlichen Person.', kzeinmalbtrg  INTEGER COMMENT 'Kennzeichen, ob Einmalbeitrag vorliegt oder nicht.', kzkoll  SMALLINT COMMENT 'Ordnet eine Produkt-/Tarif-Kombination dem Kollektiv- oder Einzelbereich zu', waehrungid  INTEGER COMMENT 'Das Attribut dient zur eindeutigen Identifizierung einer Waehrung', mandantid  INTEGER COMMENT 'Das Field mandantId enthaelt einen Schluesselwert fuer den Mandant, fuer den diese Row gue', konsortid  INTEGER COMMENT 'Das Field konsortId identifiziert die Konsortialvereinbarung, nach der dieser Vertragsteil', zustand  INTEGER COMMENT 'In diesem Attribut wird der aktuelle Status des Vertragsbausteins verschluesselt.', varianteid  STRING COMMENT 'id of SkSchadenres (Attribut zur eindeutigen Identifizierung einer Tarifvariante.)', lstver  DECIMAL(16,2) COMMENT 'Im Versicherungsvertrag tariflich vereinbarte Leistung zum Zeitpunkt des Leistungsfalls. D', leistbon  DECIMAL(16,2) COMMENT 'Bonusleistung', lstans  DECIMAL(16,2) COMMENT 'Leistung eines Vertragsteils aus dem Ansammlungsguthaben. Die Leistungen aus der verzinsli', lstsgw  DECIMAL(16,2) COMMENT 'Leistung aus Schlussueberschuessen. Die Leistungen aus Schlussueberschuss werden auf Vertr', lstfvm  DECIMAL(16,2) COMMENT 'Leistung aus uebriger versicherungstechnischer Rueckstellung', lstson  DECIMAL(16,2) COMMENT 'Sonstige Leistung', kzschadenrestod  SMALLINT COMMENT 'Kennzeichen fuer die Tod-Schadenreserve.', kzschadenresbuz  SMALLINT COMMENT 'Kennzeichen fuer die BUZ-Schadenreserve.', schrittid  INTEGER COMMENT 'Eindeutige Identifizierung eines Arbeitsschritts', lstuebbr  DECIMAL(16,2) COMMENT 'Leistung aus Ueberschuessen aus Bewertungsreserven', partkey  SMALLINT COMMENT 'the partition key', zeichjahr  TIMESTAMP COMMENT 'Zeichnungsjahr,Gruppierungsmerkmal', kzschadenrespflege  INTEGER COMMENT 'Kennzeichen fuer die Pflegerenten-Schadenreserve', kzschadenresdd  INTEGER COMMENT 'Kennzeichen fuer die Dread Disease-Schadenreserve', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Sammelkonto fuer die Schadenreserve' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');