CREATE TABLE IF NOT EXISTS ${hiveconf:database}.${hiveconf:table_name}  (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', vbid  SMALLINT COMMENT 'id of Vb (generically derived from Vb)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', ausstkost1  DECIMAL(2,2) COMMENT 'Noch ausstehende Kosten im laufenden Jahr.', ausstkost2  DECIMAL(2,2) COMMENT 'Noch ausstehende Kosten im laufenden Jahr.', ausstbtg  DECIMAL(2,2) COMMENT 'Noch ausstehende Beitraege im laufenden Jahr.', kofak1  DECIMAL(6,6) COMMENT 'Veraenderlicher Kostenfaktor 1 (Barwertabhaengig)', kofak2  DECIMAL(6,6) COMMENT 'Veraenderlicher Kostenfaktor 2 (Barwertabhaengig)', bemsue  DECIMAL(2,2) COMMENT '2) Bemessungsgroesse fuer Schlussueberschuss-Beteiligung', bemzins  DECIMAL(2,2) COMMENT '1) Bemessungsgroesse fuer laufende Zinsueberschuss-Anteile', stoabsch  DECIMAL(2,2) COMMENT 'Abschlag (absolut) der im Stornofall von der Leistung einbehalten wird.', minbtginfo  DECIMAL(2,2) COMMENT 'Vereinbarter Mindestbeitrag (fuer Kundeninformation)', regbsumkalj  DECIMAL(2,2) COMMENT '1) Regelbeitragssumme des aktuellen Kalenderjahres', gtilabkkalj  DECIMAL(6,6) COMMENT 'Anteil von Monaten, fuer die die Abschlusskosten im aktuellen Kalenderjahr getilgt sind (W', anrziverl  DECIMAL(2,2) COMMENT 'Anrechenbarer Zins in der Verlaengerungsphase', gfmin  DECIMAL(2,2) COMMENT 'Das GF-Konto ist ein Garantiefonds-Konto und dem VB zugeordnet. Befuellt wird es sowohl vo', vwkmon  DECIMAL(8,8) COMMENT 'Geplante Verwaltungskosten vom Wirkungsdatum bis zum naechsten Monatsersten. Zwischenspeic', rzresmon  DECIMAL(8,8) COMMENT 'Geplante Zinsen aus Deckungskapital vom Wirkungsdatum bis zum naechsten Monatsersten. Zwis', zidirmon  DECIMAL(8,8) COMMENT 'Geplante Zinsen aus Direktgutschrift vom Wirkungsdatum bis zum naechsten Monatsersten. Zwi', zirfbmon  DECIMAL(8,8) COMMENT 'Geplante Zinsen aus Rueckstellung fuer Beitragsrueckgewaehr vom Wirkungsdatum bis zum naec', zinsndkmon  DECIMAL(8,8) COMMENT 'Geplante Zinsen aus negativem Deckungskapital vom Wirkungsdatum bis zum naechsten Monatser', partkey  SMALLINT COMMENT 'the partition key', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Historische Ergaenzungen des Vertragsbausteins ohne Nachweisung' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;