CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}  (jahrwerteinfo_sid  STRING COMMENT 'Surrgate Key', jahrwerteinfo_id  STRING COMMENT 'Foreign Key Key table', bearbnw_sid  STRING COMMENT 'Foreign Key', lv_sid  STRING COMMENT 'Foreign Key', vt_sid  STRING COMMENT 'Foreign Key', bearbnw_id  STRING COMMENT 'Foreign Key', lv_id  STRING COMMENT 'Foreign Key', vt_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', partkey  SMALLINT COMMENT 'the partition key', foeinzausz_kum  DECIMAL(2,2) COMMENT 'Summe der Einzahlungen in Fonds im aktuellen Jahr', uebverr_kum  DECIMAL(2,2) COMMENT 'kumulierte Beitragsverrechnung im aktuellen Jahr', wterm_alt  TIMESTAMP COMMENT 'Wirksamkeitstermin der letzten Standermittlung', fvm_alt  DECIMAL(2,2) COMMENT 'Fondsvermoegen der letzten Standermittlung', res_alt  DECIMAL(2,2) COMMENT 'Reserve aus dem Bezugsvorjahr.', inkavorrat_alt  DECIMAL(2,2) COMMENT 'Inkassokostenuebertrag aus dem Bezugsvorjahr.', sueres_alt  DECIMAL(2,2) COMMENT 'Schlussueberschussreserve aus dem Bezugsvorjahr.', lstuebbr_alt  DECIMAL(2,2) COMMENT 'Leistung aus Bewertungsreserven aus dem Bezugsvorjahr.', btgsum  DECIMAL(2,2) COMMENT 'Aktuelle Summe der im Bezugsjahr gezahlten Beitraege', btguebertrag  DECIMAL(2,2) COMMENT 'Anteil an der Beitragssumme des aktuellen Kalenderjahres, der nicht im Deckungskapital zum', btguebkorr  DECIMAL(2,2) COMMENT 'Korrektur des Anteils an der Beitragssumme des aktuellen Kalenderjahres, der nicht im Deck', bearbid_alt  INTEGER COMMENT 'BearbeitungsID des Satzes aus dem Bezugsvorjahr', zuflusslues  DECIMAL(2,2) COMMENT 'Kapitalzu- bzw. abfluesse in Form von laufenden Ueberschuessen zwischen Vertragsteilen im ', dkaendlstg  DECIMAL(2,2) COMMENT 'Aenderungen im Deckungskapital durch Leistungsfaelle im Bezugsjahr', renueb_alt  DECIMAL(2,2) COMMENT 'Rentenuebertrag aus dem Bezugsvorjahr', steuzuschlag_kum  DECIMAL(2,2) COMMENT 'kumulierter Steuerzuschlag im aktuellen Jahr', btgsumgesamt  DECIMAL(2,2) COMMENT 'Summe der gezahlten Beitraege im gesamten Leben des Vertragsteils', uebverrsumgesamt  DECIMAL(2,2) COMMENT 'Summe der Beitragsverrechnungen ab Vertragsbeginn', zuzsumgesamt  DECIMAL(2,2) COMMENT 'Summe der Zuzahlungen im gesamten Leben des Vertragsteils', zulsumgesamt  DECIMAL(2,2) COMMENT 'Summe der erhaltenen Zulagen im gesamten Leben des Vertragsteils', btgsonstsumgesamt  DECIMAL(2,2) COMMENT 'Summe der sonstigen Beitraege (z.B. Anbieterwechsel) im gesamten Leben des Vertragsteils', zulkorrsumgesamt  DECIMAL(2,2) COMMENT 'Summe der Entnahmen bei Zulagenkorrekturen im gesamten Leben des Vertragsteils', rensumgesamt  DECIMAL(2,2) COMMENT 'Summe der planmaessigen Rentenzahlungen im gesamten Leben des Vertragsteils', diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(bearbidabg, lvid, vtid) disable novalidate ) COMMENT 'Werte fuer die jaehrliche Information, die im Laufe des aktuellen Jahres kumuliert wurden oder aus dem vorigen Jahr benoetigt werden.' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;