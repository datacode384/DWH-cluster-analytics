CREATE TABLE IF NOT EXISTS  core.pas_jurvt (pas_jurvt_sid  STRING COMMENT 'Surrgate Key', pas_jurvt_id  STRING COMMENT 'Foreign Key', pas_vt_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', avb1id  INTEGER COMMENT 'Erstes Schluesselfeld fuer allgemeine Vertragsbedingungen.', avb2id  INTEGER COMMENT 'Zweites Schluesselfeld fuer allgemeine Vertragsbedingungen.', avb3id  INTEGER COMMENT 'Drittes Schluesselfeld fuer allgemeine Vertragsbedingungen.', avb4id  INTEGER COMMENT 'Viertes Schluesselfeld fuer allgemeine Vertragsbedingungen.', avb5id  INTEGER COMMENT 'Fuenftes Schluesselfeld fuer allgemeine Vertragsbedingungen.', wfrist  SMALLINT COMMENT 'Dieses Attribut enthaelt bei BUZ-Vertragsteilen die vertraglich vereinbarte Wartefrist fue', staffelid  INTEGER COMMENT 'Staffelregelung fuer die Berufsunfaehigkeit', exkwegid  INTEGER COMMENT 'Vertraglich vereinbarter Zahlweg der Leistungen fuer einen Vertragsteil.', kzausl  INTEGER COMMENT 'Kennzeichen, ob auch bei Wohnsitz im Ausland Versicherungsschutz gewaehrt wird.', kzdyn  INTEGER COMMENT 'Das Kennzeichen legt fest, ob der Vertragsteil an der (auf Vertragsebene festgelegten) pla', kzkapwahl  INTEGER COMMENT 'Kennzeichnet die moeglichen Optionen bei der Kapitalwahl', bearbid  INTEGER COMMENT 'the historical version number (logId)', kzkwvereinb  INTEGER COMMENT 'Vertragsvereinbarung ueber Kapitalwahl', vttyppf  INTEGER COMMENT 'Schluesselwerte, die vier verschiedene Arten von Beitraegen auseinander halten, damit sie ', ablehngrdid  INTEGER COMMENT 'In diesem Attribut wird gegebenenfalls der Grund verschluesselt, warum ein Antrag nicht po', dtwechsel  TIMESTAMP COMMENT 'Datum, zu dem die Kuendigung wirksam wird', sumavmgges  DECIMAL(16,2) COMMENT 'Gesamtvermoegen des AVmG-Vertrags in Euro - entspricht dem Betrag der Ueberweisung an den ', sumgefkap  DECIMAL(16,2) COMMENT 'Summe gefoerdertes Kapital bis Datum der Kapitaluebertragung in Euro: zur Weiterentwicklun', sumngefkap  DECIMAL(16,2) COMMENT 'Summe ungefoerdertes Kapital bis Datum der Kapitaluebertragung in Euro: zur Weiterentwickl', sumzulges  DECIMAL(16,2) COMMENT 'Summe der gesamten Zulagen in Euro - basierend auf bisher erhaltenen Zahlungen (ZARA, nich', btgjahr  INTEGER COMMENT 'Beitragsjahr der Zulagengewaehrung', beiwechseljahr  DECIMAL(16,2) COMMENT 'Beitraege im Beitragsjahr in Euro (wie AZ01): Beitrag Wechseljahr (BEITRAG_WJ).', sumbeivorjahre  DECIMAL(16,2) COMMENT 'Beitraege im Beitragsjahr in Euro (wie AZ01): Beitragssumme fuer alle VJ (SUM_BEITRAG_VJ).', beizulwechseljahr  DECIMAL(16,2) COMMENT 'Summe der Altersvorsorgebeitraege im sinne des § 82, auf die § 10a oder Abschnitt XI EStG ', sumbeizulvorjahre  DECIMAL(16,2) COMMENT 'Summe der Altersvorsorgebeitraege im Sinne des § 82, auf die § 10a oder Abschnitt XI EStG ', vorvtrnr  STRING COMMENT 'Vorvertragsnummer', gefke  DECIMAL(16,2) COMMENT 'Kapitalertrag aus gefoerdertem Kapital.', ngefke  DECIMAL(16,2) COMMENT 'Kapitalertrag aus nicht gefoerdertem Kapital.', kzabfindungkbr  INTEGER COMMENT 'Wird gesetzt, wenn im Geschaeftsprozess ""Vorbereitung Verrentung Riester"" die Abfindung ', kzabfindungmgl  INTEGER COMMENT 'Wird gesetzt, wenn im Geschaeftsprozess ""Simulation Ablauf"" ermittelt wird, dass die Abf', sumuebkap  DECIMAL(16,2) COMMENT 'Summe uebertragenes Kapital.', lvidextneu  STRING COMMENT 'lvIdExt des neuen Vertrags', pdidneu  STRING COMMENT 'pdId des neuen Vertrags', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', zeichjahrjur  TIMESTAMP COMMENT 'Zeichnungsjahr (juristisch)', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid, vtid) disable novalidate ) COMMENT 'Die Rows in Table jurVT repraesentieren jeweils genau einen Vertragsteil.' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');