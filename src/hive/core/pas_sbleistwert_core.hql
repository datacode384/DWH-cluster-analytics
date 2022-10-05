CREATE TABLE IF NOT EXISTS  core.pas_sbleistwert (pas_sbleistwert_sid  STRING COMMENT 'Surrgate Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbid  INTEGER COMMENT 'the historical version number (logId)', pas_anrewerte_id  STRING COMMENT 'Foreign Key', wirkdat  TIMESTAMP COMMENT 'Termin, zu dem eine aenderung wirksam wird.', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', feindat  SMALLINT COMMENT 'Feinsteuerungsinformation zum Termin Termin-Wirksamkeit', sbid  INTEGER COMMENT 'id of SbLeistWert (Eindeutige Kennzeichnung einer Sammelbewegung)', standid  INTEGER COMMENT 'Zuordnung einer Bewegung zu einem (historischen) Vertragsstand', bearbdat  TIMESTAMP COMMENT 'Termin, an dem das Objekt bearbeitet wurde.', bwggrdid  INTEGER COMMENT 'Eindeutige Identifikation der Ursache fuer bestimmte Kontobewegungen, die in Form einer Sa', bearbidsto  INTEGER COMMENT 'Wird eine Bearbeitung storniert, enthaelt die bearbIdSto des aktuellen Vorgangs die bearbI', lstver  DECIMAL(16,2) COMMENT 'Im Versicherungsvertrag tariflich vereinbarte Leistung zum Zeitpunkt des Leistungsfalls. D', leistbon  DECIMAL(16,2) COMMENT 'Bonusleistung', lstans  DECIMAL(16,2) COMMENT 'Leistung eines Vertragsteils aus dem Ansammlungsguthaben. Die Leistungen aus der verzinsli', lstsgw  DECIMAL(16,2) COMMENT 'Leistung aus Schlussueberschuessen. Die Leistungen aus Schlussueberschuss werden auf Vertr', latodbon  DECIMAL(16,2) COMMENT 'Todesfalleistung Bonus', zbrat  DECIMAL(16,2) COMMENT 'Anteiliger Zahlbeitrag fuer ein beliebiges Intervall &lt;= Ratenzahlungsabschnitt', uebverrrat  DECIMAL(16,2) COMMENT 'Anteilige Beitragsverrechnung fuer ein beliebiges Intervall &lt;= Ratenzahlungsabschnitt', pdid  STRING COMMENT 'Eindeutige Identifikation des Produkts, das dem LV-Vertrag zugeordnet ist.', tfid  STRING COMMENT 'Eindeutiger Identifikator eines LV-Tarifs', sex  INTEGER COMMENT 'Identifizierender Schluessel fuer das Geschlecht einer natuerlichen Person.', kzeinmalbtrg  INTEGER COMMENT 'Kennzeichen, ob Einmalbeitrag vorliegt oder nicht.', kzkoll  SMALLINT COMMENT 'Ordnet eine Produkt-/Tarif-Kombination dem Kollektiv- oder Einzelbereich zu', waehrungid  INTEGER COMMENT 'Das Attribut dient zur eindeutigen Identifizierung einer Waehrung', mandantid  INTEGER COMMENT 'Das Field mandantId enthaelt einen Schluesselwert fuer den Mandant, fuer den diese Row gue', konsortid  INTEGER COMMENT 'Das Field konsortId identifiziert die Konsortialvereinbarung, nach der dieser Vertragsteil', zustand  INTEGER COMMENT 'In diesem Attribut wird der aktuelle Status des Vertragsbausteins verschluesselt.', varianteid  STRING COMMENT 'id of SbLeistWert (Attribut zur eindeutigen Identifizierung einer Tarifvariante.)', lstfvm  DECIMAL(16,2) COMMENT 'Leistung aus uebriger versicherungstechnischer Rueckstellung', lstrefi  DECIMAL(16,2) COMMENT 'Leistung aus Refinanzierung', lstuebbr  DECIMAL(16,2) COMMENT 'Gesamte Leistung aus Bewertungsreserven', stornoabschlag  DECIMAL(16,2) COMMENT 'Stornoabschlag', partkey  SMALLINT COMMENT 'the partition key', zeichjahr  TIMESTAMP COMMENT 'Zeichnungsjahr,Gruppierungsmerkmal', stornoabschlagva  DECIMAL(16,2) COMMENT 'Stornoabschlag auf das Ansammlungsguthaben', lstuebbrmin  DECIMAL(16,2) COMMENT 'Leistung aus Mindestbeteiligung an Bewertungsreserven', lstuebbrzus  DECIMAL(16,2) COMMENT 'Zusaetzliche Leistung aus Bewertungsreserven', lstzeitwert  DECIMAL(15,2) COMMENT 'Leistung aus dem Zeitwert der Garantie', lstzuskto  DECIMAL(15,2) COMMENT 'Leistung aus dem Zusatzkonto', lstverrkto  DECIMAL(15,2) COMMENT 'Leistung aus Verrechnungskonten', lstsgwfonds  DECIMAL(16,2) COMMENT 'Leistung aus fondsgebundenen Schlussueberschuessen', nachsteuer  DECIMAL(15,2) COMMENT 'Steuerbetrag aus der Nachversteuerung von Praemien', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid, vtid, sbid, varianteid) disable novalidate ) COMMENT 'Sammelbewegung Leistungswerte Sammelbewegung Leistungswerte zur Fuehrung von Auszahlungsbetraegen je Quelle (garantierte Leistung, laufender ueberschuss, Schlussueberschuss) im Rahmen von Leistungsbearbeitungen.Gruppierungsmerkmale: Fuer die Rechnungsleg' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');