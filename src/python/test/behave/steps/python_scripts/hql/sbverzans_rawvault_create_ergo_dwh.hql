CREATE TABLE IF NOT EXISTS ${hiveconf:database}.${hiveconf:table_name}  (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbid  INTEGER COMMENT 'the historical version number (logId)', wirkbeginn  TIMESTAMP COMMENT 'Termin, ab dem der neue Vertragsstand fachlich wirksam ist.', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', feinbeginn  SMALLINT COMMENT 'Feinsteuerungsinformation zum Termin Termin-Wirksamkeit-Beginn', sbid  INTEGER COMMENT 'id of SbVerzAns (Eindeutige Kennzeichnung einer Sammelbewegung)', standid  INTEGER COMMENT 'Zuordnung einer Bewegung zu einem (historischen) Vertragsstand', wirkende  TIMESTAMP COMMENT 'Termin, ab dem der Vertragsstand fachlich nicht mehr wirksam ist.', bwggrdid  INTEGER COMMENT 'Eindeutige Identifikation der Ursache fuer bestimmte Kontobewegungen, die in Form einer Sa', bearbidsto  INTEGER COMMENT 'Wird eine Bearbeitung storniert, enthaelt die bearbIdSto des aktuellen Vorgangs die bearbI', anzir  DECIMAL(2,2) COMMENT 'Absolutbetrag der Zinsen auf das verzinsliche Ansammlungsguthaben', feinende  SMALLINT COMMENT 'Feinsteuerungsinformation zum Termin Termin-Wirksamkeit-Ende', anzidir  DECIMAL(2,2) COMMENT 'Zinsen auf das Ansammlungsguthaben aus Direktgutschrift', bearbdat  TIMESTAMP COMMENT 'Termin, an dem das Objekt bearbeitet wurde.', anzirfb  DECIMAL(2,2) COMMENT 'Zinsen auf das Ansammlungsguthaben aus der RfB', vwkans  DECIMAL(2,2) COMMENT 'Verwaltungskosten auf das verzinsliche Ansammlungsguthaben bei Verzinsung des Ansammlungsg', pdid  STRING COMMENT 'Eindeutige Identifikation des Produkts, das dem LV-Vertrag zugeordnet ist.', tfid  STRING COMMENT 'Eindeutiger Identifikator eines LV-Tarifs', sex  INTEGER COMMENT 'Identifizierender Schluessel fuer das Geschlecht einer natuerlichen Person.', kzeinmalbtrg  INTEGER COMMENT 'Kennzeichen, ob Einmalbeitrag vorliegt oder nicht.', kzkoll  SMALLINT COMMENT 'Ordnet eine Produkt-/Tarif-Kombination dem Kollektiv- oder Einzelbereich zu', waehrungid  INTEGER COMMENT 'Das Attribut dient zur eindeutigen Identifizierung einer Waehrung', mandantid  INTEGER COMMENT 'Das Field mandantId enthaelt einen Schluesselwert fuer den Mandant, fuer den diese Row gue', konsortid  INTEGER COMMENT 'Das Field konsortId identifiziert die Konsortialvereinbarung, nach der dieser Vertragsteil', zustand  INTEGER COMMENT 'In diesem Attribut wird der aktuelle Status des Vertragsbausteins verschluesselt.', varianteid  STRING COMMENT 'id of SbVerzAns (Attribut zur eindeutigen Identifizierung einer Tarifvariante.)', refizir  DECIMAL(2,2) COMMENT 'Rechnungsmaessige Zinsen auf das Refinanzierungskonto', partkey  SMALLINT COMMENT 'the partition key', zeichjahr  TIMESTAMP COMMENT 'Zeichnungsjahr,Gruppierungsmerkmal', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Sammelbewegung Verzinsung Ansammlungsguthaben Sammelbewegung zur Dokumentation der Verzinsung des Ansammlungsguthabens, aufgegliedert nach Finanzierungsquelle (rechnungsmaessig, Direktgutschrift, aus RfB)Gruppierungsmerkmale: Fuer die Rechnungslegung rel' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;