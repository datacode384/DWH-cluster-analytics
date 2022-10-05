CREATE TABLE IF NOT EXISTS  raw_zone.pas_sbquotausgl (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', wirkbeginn  TIMESTAMP COMMENT 'Termin, ab dem der neue Vertragsstand fachlich wirksam ist.', sbid  INTEGER COMMENT 'id of SbQuotAusgl (Eindeutige Kennzeichnung einer Sammelbewegung)', feinbeginn  SMALLINT COMMENT 'Feinsteuerungsinformation zum Termin Termin-Wirksamkeit-Beginn', konspartid  INTEGER COMMENT 'id of SbQuotAusgl (Das Field konsPartId identifiziert den Konsortialpartner.)', wirkende  TIMESTAMP COMMENT 'Termin, ab dem der Vertragsstand fachlich nicht mehr wirksam ist.', bearbid  INTEGER COMMENT 'the historical version number (logId)', feinende  SMALLINT COMMENT 'Feinsteuerungsinformation zum Termin Termin-Wirksamkeit-Ende', bearbidsto  INTEGER COMMENT 'Wird eine Bearbeitung storniert, enthaelt die bearbIdSto des aktuellen Vorgangs die bearbI', bearbdat  TIMESTAMP COMMENT 'Termin, an dem das Objekt bearbeitet wurde.', quotausglproperty  INTEGER COMMENT 'Schluesselverzeichnis fuer quotalen Ausgleich', quotausglvalue  DECIMAL(16,2) COMMENT 'Der Wert der Property (das sind immer Geldbetraege)', standid  INTEGER COMMENT 'Zuordnung einer Bewegung zu einem (historischen) Vertragsstand', bwggrdid  INTEGER COMMENT 'Eindeutige Identifikation der Ursache fuer bestimmte Kontobewegungen, die in Form einer Sa', pdid  STRING COMMENT 'Eindeutige Identifikation des Produkts, das dem LV-Vertrag zugeordnet ist.', tfid  STRING COMMENT 'Eindeutiger Identifikator eines LV-Tarifs', sex  INTEGER COMMENT 'Identifizierender Schluessel fuer das Geschlecht einer natuerlichen Person.', kzeinmalbtrg  INTEGER COMMENT 'Kennzeichen, ob Einmalbeitrag vorliegt oder nicht.', kzkoll  SMALLINT COMMENT 'Ordnet eine Produkt-/Tarif-Kombination dem Kollektiv- oder Einzelbereich zu', waehrungid  INTEGER COMMENT 'Das Attribut dient zur eindeutigen Identifizierung einer Waehrung', mandantid  INTEGER COMMENT 'Das Field mandantId enthaelt einen Schluesselwert fuer den Mandant, fuer den diese Row gue', konsortid  INTEGER COMMENT 'Das Field konsortId identifiziert die Konsortialvereinbarung, nach der dieser Vertragsteil', zustand  INTEGER COMMENT 'In diesem Attribut wird der aktuelle Status des Vertragsbausteins verschluesselt.', varianteid  STRING COMMENT 'id of SbQuotAusgl (Attribut zur eindeutigen Identifizierung einer Tarifvariante.)', partkey  SMALLINT COMMENT 'the partition key', zeichjahr  TIMESTAMP COMMENT 'Zeichnungsjahr,Gruppierungsmerkmal', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'sbQuotAusgl' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');