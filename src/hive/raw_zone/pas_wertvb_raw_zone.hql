CREATE TABLE IF NOT EXISTS  raw_zone.pas_wertvb (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbid  INTEGER COMMENT 'the historical version number (logId)', vbid  SMALLINT COMMENT 'id of Vb (generically derived from Vb)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', anrrkw  DECIMAL(16,2) COMMENT 'Anrechenbarer Wert Rueckkauf', anrbfr  DECIMAL(16,2) COMMENT 'Anrechenbarer Wert fuer die Beitragsfreistellung', gbanrwert  DECIMAL(16,2) COMMENT 'Anrechenbarer Wert fuer den Garantiereservebeitrag', tbanrwert  DECIMAL(16,2) COMMENT 'Anrechenbarer Wert fuer den Tarifbeitrag', nbanrwert  DECIMAL(16,2) COMMENT 'Anrechenbarer Wert fuer den Normbeitrag', sgafonds  DECIMAL(16,2) COMMENT 'Schlussueberschussfonds', tbbtgsum  DECIMAL(16,2) COMMENT 'bisherige Tarifbeitragsumme', zbbtgsum  DECIMAL(16,2) COMMENT 'bisherige Zahlbeitragsumme', pbbtgsum  DECIMAL(16,2) COMMENT 'biherige Provisionsbeitragsumme', akztbfin  DECIMAL(16,2) COMMENT 'finanzierte gezillmerte Abschlusskosten fuer den Tarifbeitrag', aknztbfin  DECIMAL(16,2) COMMENT 'finanzierte nicht-gezillmerte Abschlusskosten fuer den Tarifbeitrag', akztbalt  DECIMAL(16,2) COMMENT 'alte gezillmerte Abschlusskosten fuer den Tarifbeitrag', aknztbalt  DECIMAL(16,2) COMMENT 'alte nicht-gezillmerte Abschlusskosten fuer den Tarifbeitrag', akznbfin  DECIMAL(16,2) COMMENT 'finanzierte gezillmerte Abschlusskosten fuer den Normbeitrag', aknznbfin  DECIMAL(16,2) COMMENT 'finanzierte nicht-gezillmerte Abschlusskosten fuer den Normbeitrag', akznbalt  DECIMAL(16,2) COMMENT 'alte gezillmerte Abschlusskosten fuer den Normbeitrag', aknznbalt  DECIMAL(16,2) COMMENT 'alte nicht-gezillmerte Abschlusskosten fuer den Normbeitrag', lakor  DECIMAL(16,2) COMMENT 'Notwendig bei technischen aenderungen von Rueckgewaehrtarifen', basres  DECIMAL(16,2) COMMENT 'Basisreserve', tarres  DECIMAL(16,2) COMMENT 'Tarifreserve', nrmres  DECIMAL(16,2) COMMENT 'Normreserve', garres  DECIMAL(16,2) COMMENT 'Garantiereserve', inkakor  DECIMAL(16,2) COMMENT 'inkaKor', ratzuakor  DECIMAL(16,2) COMMENT 'ratzuaKor', vstkkkor  DECIMAL(16,2) COMMENT 'vstkkKor', vtsaminkkor  DECIMAL(16,2) COMMENT 'vtsaminkKor', bewprovsum  DECIMAL(16,2) COMMENT 'bisherige Provisions-Bewertungssumme', dauerbeitrmrest  INTEGER COMMENT 'restliche Beitragszahlungsdauer in Monaten', minbtgsumakt  DECIMAL(16,2) COMMENT 'Das Attribut enthaelt die vereinbarte Regelbeitragssumme bis zum Berechnungszeitpunkt. Auf', bildr  DECIMAL(16,2) COMMENT 'BilDR als Basis fuer Stichtagsbewegungen', akkalkvol  DECIMAL(16,2) COMMENT 'Kalkulatorisches Abschlusskostenvolumen bei kontenbasierten Tarifen', akbilvol  DECIMAL(16,2) COMMENT 'Bilanzielles Abschlusskostenvolumen bei kontenbasierten Tarifen', akkalkfin  DECIMAL(16,2) COMMENT 'finanzierte kalkulatorische Abschlusskosten bei kontenbasierten Tarifen', akbilfin  DECIMAL(16,2) COMMENT 'Finanzierte bilanzielle Abschlusskosten bei kontenbasierten Tarifen', deltabildr  DECIMAL(16,2) COMMENT 'deltaBilDR als Basis zur Berechnung des Auffuellbedarfs', ratzuavwkkor  DECIMAL(16,2) COMMENT 'Korrektur des Ratenzuschlags fuer Verwaltungskosten bei technischen Aenderungen', ratzuazakor  DECIMAL(16,2) COMMENT 'Korrektur des Ratenzuschlags fuer Zinsausfall bei technischen Aenderungen', ratzuaritodkor  DECIMAL(16,2) COMMENT 'Korrektur des Ratenzuschlags fuer Risiko Tod bei technischen Aenderungen', ratzuarisonkor  DECIMAL(16,2) COMMENT 'Korrektur des Ratenzuschlags fuer sonstige Risiken bei technischen Aenderungen', akmrkwvol  DECIMAL(16,2) COMMENT 'Abschlusskostenvolumen fuer Garantiereserve als Bezugsgroesse fuer Mindestrueckkaufswerte', akmrkwfin  DECIMAL(16,2) COMMENT 'Finanzierte Abschlusskosten fuer Garantiereserve als Bezugsgroesse fuer Mindestrueckkaufsw', partkey  SMALLINT COMMENT 'the partition key', zillres  DECIMAL(16,2) COMMENT 'gezillmerte Nettoreserve (Zillmerreserve)', nettores  DECIMAL(16,2) COMMENT '(ungezillmerte) Nettoreserve', zillbanrwert  DECIMAL(16,2) COMMENT 'Anrechenbarer Wert fuer den Zillmerreservebeitrag', nettobanrwert  DECIMAL(16,2) COMMENT 'Anrechenbarer Wert fuer den Nettoreservebeitrag', aktanspr  DECIMAL(16,2) COMMENT 'aktivierte Ansprueche als Werststand zur Bestimmung des Abschlusskostenverlustes', aktansprkoba  DECIMAL(16,2) COMMENT 'aktivierte Ansprueche aus Zillmerung des kontenbasierten Anteils eines Tarifes als Werstst', fvm  DECIMAL(16,2) COMMENT 'Fondsvermoegen fuer sonstige versicherungstechnische Rueckstellung', drfvm  DECIMAL(16,2) COMMENT 'Fondsvermoegen fuer Deckungsrueckstellung', anrbfrkonten  DECIMAL(16,2) COMMENT 'Anrechenbarer Wert fuer die Beitragsfreistellung aus kontenbasiertem Vermoegen', stoabbfrkonten  DECIMAL(16,2) COMMENT 'Stornoabschlag bei Beitragsfreistellung zur Entnahme aus kontenbasiertem Vermoegen', stoabdkbezrestalt  DECIMAL(16,2) COMMENT 'Bezugsgroesse fuer die Berechnung von Stornoabschlag auf DK vor TAE', stoabfvmbezrestalt  DECIMAL(16,2) COMMENT 'Bezugsgroesse fuer die Berechnung von Stornoabschlag auf FVM vor TAE', stoabakdbezrestalt  DECIMAL(16,2) COMMENT 'Bezugsgroesse fuer die Berechnung von Stornoabschlag auf klassichen Abschlusskosten vor TA', stoabakfbezrestalt  DECIMAL(16,2) COMMENT 'Bezugsgroesse fuer die Berechnung von Stornoabschlag auf kontenbasierten Abschlusskosten v', stoabdkbfr  DECIMAL(16,2) COMMENT 'Stornoabschlag auf DK bei der Beitragsfreistellung', stoabfvmbfr  DECIMAL(16,2) COMMENT 'Stornoabschlag auf Fvm bei der Beitragsfreistellung', steuwertstand  DECIMAL(16,2) COMMENT 'gesamter steuerlicher Wertstand aus KESt-Sicht', auffllerlkl  DECIMAL(16,2) COMMENT 'Auffuellung bei Typ Erleben, klassisch', auffllzinskl  DECIMAL(16,2) COMMENT 'Auffuellung bei Typ Zins, klassisch', auffllbukl  DECIMAL(16,2) COMMENT 'Auffuellung bei Typ BU, klassisch', auffllerldrfvm  DECIMAL(16,2) COMMENT 'Auffuellung bei Typ Erleben, Fondsvermoegen', auffllzinsdrfvm  DECIMAL(16,2) COMMENT 'Auffuellung bei Typ Zins, Fondsvermoegen', auffllzinsva  DECIMAL(16,2) COMMENT 'Auffuellung bei Typ Zins, Verzinsliche Ansammlung', auffllraa  DECIMAL(16,2) COMMENT 'Reduktion aktivierbarer Ansprueche durch Auffuellung', bezrefires  DECIMAL(16,2) COMMENT 'Reserve zur konstruktiven Neuberechnung der Leistung aus Kontostand BEZREFI_LA1', auffllgdr  DECIMAL(16,2) COMMENT 'Differenz zwischen Reserve incl. Zusatzreserve (Bilanzdeckungsrueckstellung nach den neuen', auffllgaa  DECIMAL(16,2) COMMENT 'Differenz zwischen aktivierbaren Anspruechen nach den alten und den neuen Rechnungsgrundla', akztbfinbfr  DECIMAL(16,2) COMMENT 'finanzierte gezillmerte Abschlusskosten fuer den Tarifbeitrag bei Beitragsfreistellung', vwkres  DECIMAL(16,2) COMMENT 'Verwaltungskostenreserve', minantfonds  DECIMAL(16,2) COMMENT 'Mindestanteil zu Ueberschuessen aus Bewertungsreserven', auffllbilrz  DECIMAL(16,2) COMMENT 'Auffuellung fuer Nachreservierungstyp BILRZ', auffllbilrzkb  DECIMAL(16,2) COMMENT 'Auffuellung fuer Nachreservierungstyp BILRZ kontenbasiert', stoabrkwdk  DECIMAL(16,2) COMMENT 'Stornoabschlag auf Deckungskapital bei Rueckkauf im Rahmen einer Beitragsfreistellung', stoabrkwkonten  DECIMAL(16,2) COMMENT 'Stornoabschlag auf kontenbasiertes Vermoegen bei Rueckkauf im Rahmen einer Beitragsfreiste', auffllzinsvaren  DECIMAL(15,2) COMMENT 'Auffuellung bei Typ Zins, Verzinsliche Ansammlung, aus Rentenbezugszeit herruehrend', auffllerlva  DECIMAL(15,2) COMMENT 'Auffuellung bei Typ Erleben, Verzinsliche Ansammlung', auffllerlvaren  DECIMAL(15,2) COMMENT 'Auffuellung bei Typ Erleben, Verzinsliche Ansammlung, aus Rentenbezugszeit herruehrend', akzkalkbisher  DECIMAL(15,2) COMMENT 'Bisheriger Anteil der klassischen gezillmerten kalkulatorischen AK', aknzkalkbisher  DECIMAL(15,2) COMMENT 'Bisheriger Anteil der klassischen nicht gezillmerten kalkulatorischen AK', akzbilbisher  DECIMAL(15,2) COMMENT 'Bisheriger Anteil der klassischen gezillmerten bilanziellen AK', aknzbilbisher  DECIMAL(15,2) COMMENT 'Bisheriger Anteil der klassischen nicht gezillmerten bilanziellen AK', akmrkwbisher  DECIMAL(15,2) COMMENT 'Bisheriger Anteil der AK fuer Mindestrueckkaufswert', akkalkvolbisher  DECIMAL(15,2) COMMENT 'Bisheriger Anteil des kalkulatorischen Abschlusskostenvolumens bei kontenbasierten Tarifen', akbilvolbisher  DECIMAL(15,2) COMMENT 'Bisheriger Anteil des bilanziellen Abschlusskostenvolumens bei kontenbasierten Tarifen', akkalkfondsfin  DECIMAL(15,2) COMMENT 'Finanzierte fondsspezifische kalkulatorische Abschlusskosten, falls Abschlusskosten auf ei', akkalkfondsvol  DECIMAL(15,2) COMMENT 'Fondsspezifisches kalkulatorisches Abschlusskostenvolumen bei kontenbasierten Tarifen, fal', steuzuschlaggezsum  DECIMAL(15,2) COMMENT 'Summe der bisher gezahlten Steuer auf Praemien des Vertragsbausteins', auffllraaerl  DECIMAL(15,2) COMMENT 'Reduktion aktivierbarer Ansprueche durch Auffuellung bei Nachreservierung Erleben', auffllraazins  DECIMAL(15,2) COMMENT 'Reduktion aktivierbarer Ansprueche durch Auffuellung bei Nachreservierung Zins (Zinszusatz', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Wertstandsgroessen auf VB - EbeneWertstand-VB: Wertstandsgroessen auf VB - EbeneHistorie-streng: Attribute, die zur strengen Historienfuehrung am Vertrag benoetigt werden' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');