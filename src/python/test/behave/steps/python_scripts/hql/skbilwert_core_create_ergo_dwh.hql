CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}  (skbilwert_sid  STRING COMMENT 'Surrgate Key', skbilwert_id  STRING COMMENT 'Foreign Key Key table', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbnw_sid  STRING COMMENT 'Foreign Key', lv_sid  STRING COMMENT 'Foreign Key', vt_sid  STRING COMMENT 'Foreign Key', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbnw_id  STRING COMMENT 'Foreign Key', lv_id  STRING COMMENT 'Foreign Key', vt_id  STRING COMMENT 'Foreign Key', bearbdatsto  TIMESTAMP COMMENT 'Bearbeitungstermin des stornierenden Vorgangs', bilaa  DECIMAL(2,2) COMMENT 'Aktivierte Ansprueche zum Bilanztermin.', bilnrmres  DECIMAL(2,2) COMMENT 'Normreserve am Bilanztermin', biltarres  DECIMAL(2,2) COMMENT 'Tarifreserve am Bilanztermin', bilbub  DECIMAL(2,2) COMMENT 'Beitragsuebertrag zum Bilanztermin.', bildr  DECIMAL(2,2) COMMENT 'Das Deckungskapital ist eine Rechengroesse, die den Wert eines Vertrages bzw. eines Vertra', bonres  DECIMAL(2,2) COMMENT 'Bonusreserve', statanz  INTEGER COMMENT 'Statistische Anzahl (z.B. zum Bilanztermin)', statanzbfr  INTEGER COMMENT 'Statistische Anzahl beitragsfreier Vertraege (z.B. zum Bilanztermin)', statbeitr  DECIMAL(2,2) COMMENT 'Statistischer Beitrag (z.B. zum Bilanztermin)', statvsum  DECIMAL(2,2) COMMENT 'Statistische Versicherungssumme (z.B. zum Bilanztermin)', statvsumbfr  DECIMAL(2,2) COMMENT 'Beitragsfreie statistische Versicherungssumme (z.B. zum Bilanztermin)', risikosumme  DECIMAL(2,2) COMMENT 'Pro VT oder VB unter Risiko stehende Summe (""brutto"", d.h. ohne Abzug von DK o.ae.)', riskap1  DECIMAL(2,2) COMMENT 'Riskiertes Kapital bezueglich der ersten versicherten Risikoart.', riskap2  DECIMAL(2,2) COMMENT 'Riskiertes Kapital bezueglich der zweiten versicherten Risikoart.', bilsgr  DECIMAL(2,2) COMMENT 'Rueckstellung fuer den Schlussueberschuss', gebsgrb  DECIMAL(2,2) COMMENT 'gebundene Rueckstellung fuer den Schlussueberschuss', btgrest  DECIMAL(2,2) COMMENT 'Die Differenz zwischen sollgestelltem und inkassiertem Beitrag.', zusrkstrente  DECIMAL(2,2) COMMENT 'Rueckstellung fuer einen pauschal kalkulierten Rentenzuschlag', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', wirkdat  TIMESTAMP COMMENT 'Termin, zu dem eine aenderung wirksam wird.', feindat  SMALLINT COMMENT 'Feinsteuerungsinformation zum Termin Termin-Wirksamkeit', bearbdat  TIMESTAMP COMMENT 'Termin, an dem das Objekt bearbeitet wurde.', pdid  STRING COMMENT 'Eindeutige Identifikation des Produkts, das dem LV-Vertrag zugeordnet ist.', tfid  STRING COMMENT 'Eindeutiger Identifikator eines LV-Tarifs', sex  INTEGER COMMENT 'Identifizierender Schluessel fuer das Geschlecht einer natuerlichen Person.', kzeinmalbtrg  INTEGER COMMENT 'Kennzeichen, ob Einmalbeitrag vorliegt oder nicht.', kzkoll  SMALLINT COMMENT 'Ordnet eine Produkt-/Tarif-Kombination dem Kollektiv- oder Einzelbereich zu', waehrungid  INTEGER COMMENT 'Das Attribut dient zur eindeutigen Identifizierung einer Waehrung', mandantid  INTEGER COMMENT 'Das Field mandantId enthaelt einen Schluesselwert fuer den Mandant, fuer den diese Row gue', konsortid  INTEGER COMMENT 'Das Field konsortId identifiziert die Konsortialvereinbarung, nach der dieser Vertragsteil', zustand  INTEGER COMMENT 'In diesem Attribut wird der aktuelle Status des Vertragsbausteins verschluesselt.', statzbsum  DECIMAL(2,2) COMMENT 'statistische Beitragssumme', statzbsumbfr  DECIMAL(2,2) COMMENT 'statistische beitragsfreie Beitragssumme', varianteid  STRING COMMENT 'id of SkBilWert (Attribut zur eindeutigen Identifizierung einer Tarifvariante.)', kzschadenresbuz  SMALLINT COMMENT 'Kennzeichen fuer die BUZ-Schadenreserve.', kzschadenrestod  SMALLINT COMMENT 'Kennzeichen fuer die Tod-Schadenreserve.', negbildr  DECIMAL(2,2) COMMENT 'Kann ein Teil einer negativen Basisreserve nicht aktiviert werden, so wird dieser Teil hie', basisuebbr  DECIMAL(2,2) COMMENT 'einzelvertragliche Basisgroesse zur Berechnung der Ueberschuesse aus Bewertungsreserven', partkey  SMALLINT COMMENT 'the partition key', bilzillres  DECIMAL(2,2) COMMENT 'gezillmerte Nettoreserve (Zillmerreserve) am Bilanztermin', bilnettores  DECIMAL(2,2) COMMENT '(ungezillmerte) Nettoreserve am Bilanztermin', bonnettores  DECIMAL(2,2) COMMENT 'Bonus-Nettoreserve am Bilanztermin', bilvwkres  DECIMAL(2,2) COMMENT 'Verwaltungskostenreserve am Bilanztermin', bonvwkres  DECIMAL(2,2) COMMENT 'Bonus-Verwaltungskostenreserve am Bilanztermin', bilnettobub  DECIMAL(2,2) COMMENT 'Netto-Beitragsuebertrag zum Bilanztermin', zeichjahr  TIMESTAMP COMMENT 'Zeichnungsjahr,Gruppierungsmerkmal', bilnachres  DECIMAL(2,2) COMMENT 'Reserve mit den Rechnungsgrundlagen fuer die Nachreservierung am Bilanztermin', bilauffuellbasis  DECIMAL(2,2) COMMENT 'Reserve mit den Rechnungsgrundlagen fuer die Bilanzierung zum Beginn der Nachreservierung ', gebsueafuebbr  DECIMAL(2,2) COMMENT 'Gebundene Rueckstellung fuer die gesamte Leistung aus Bewertungsreserven', bilsueafuebbr  DECIMAL(2,2) COMMENT 'freie Rueckstellung (frei innerhalb der RfB) fuer die Leistung aus Bewertungsreserven, i.d', bilaakoba  DECIMAL(2,2) COMMENT 'Aktivierte Ansprueche aus Zillmerung von Fondsguthaben zum Bilanztermin.', aktivwert  DECIMAL(8,8) COMMENT 'Bei Rueckdeckungsversicherungen vom VN am Wirksamkeitstermin zu aktivierender Wert.', aktivwertp1mon  DECIMAL(8,8) COMMENT 'Bei Rueckdeckungsversicherungen vom VN einen Monat nach dem Wirksamkeitstermin zu aktivier', psvwertp1mon  DECIMAL(8,8) COMMENT 'Bemessungsgrundlage des Beitrags zum Pensions-Sicherungs-Verein (PSV) bei Direktversicheru', psvwert  DECIMAL(8,8) COMMENT 'Bemessungsgrundlage des Beitrags zum Pensions-Sicherungs-Verein (PSV) bei Direktversicheru', kumauffuellbedarf  DECIMAL(2,2) COMMENT 'Kumulierter Auffuellungsbedarf bei Renten fuer die Neubewertung aufgrund der neuen Sterbet', refikap  DECIMAL(2,2) COMMENT 'Refinanzierungskapital', wertstand  DECIMAL(2,2) COMMENT 'Wertstand des Vertragsteils (ohne Beitragsuebertrag)', auffllerlkl  DECIMAL(2,2) COMMENT 'Klassische Auffuellung fuer Nachreservierungstyp Erleben (ERL)', auffllzinskl  DECIMAL(2,2) COMMENT 'Klassische Auffuellung fuer Nachreservierungstyp Zinszusatzreserve (ZINS)', auffllbukl  DECIMAL(2,2) COMMENT 'Klassische Auffuellung fuer Nachreservierungstyp Berufsunfaehigkeit (BU)', auffllzinsva  DECIMAL(2,2) COMMENT 'Klassische Auffuellung fuer Verzinslicher Ansammlung fuer Nachreservierungstyp Zinszusatzr', auffllraa  DECIMAL(2,2) COMMENT 'Reduktion aktivierbarer Ansprueche aus Auffuellung bei Nachreservierung', auffllgdr  DECIMAL(2,2) COMMENT 'Differenz zwischen Reserve incl. Zusatzreserve (Bilanzdeckungsrueckstellung nach den neuen', auffllgaa  DECIMAL(2,2) COMMENT 'Differenz zwischen aktivierbaren Anspruechen nach den alten und den neuen Rechnungsgrundla', ausglherlkl  DECIMAL(2,2) COMMENT 'Bei Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastung durch die Neu', ausglhzinslkl  DECIMAL(2,2) COMMENT 'Bei Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastung durch die Neu', ausglhzinsva  DECIMAL(2,2) COMMENT 'Bei Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastung durch die Neu', bilsgrredrefi  DECIMAL(2,2) COMMENT 'Kuerzung der Schlussueberschussreserve bei Refinanzierung', gebsgrbredrefi  DECIMAL(2,2) COMMENT 'Kuerzung der gebundenen Schlussueberschussreserve bei Refinanzierung', aufstockbg  DECIMAL(2,2) COMMENT 'Aufstockung der Deckungsrueckstellung wegen Beitragsgarantie (beim Sollstellungsriester un', checkdr  DECIMAL(2,2) COMMENT 'Kontrollreserve', checkdrvgl  DECIMAL(2,2) COMMENT 'Vergleichsgroesse zur Kontrollreserve', gebsueafuebbrmin  DECIMAL(2,2) COMMENT 'Gebundene Rueckstellung fuer die Leistung aus Mindestbeteiligung an Bewertungsreserven', gebsueafuebbrzus  DECIMAL(2,2) COMMENT 'Gebundene Rueckstellung fuer die zusaetzliche Leistung aus Bewertungsreserven', basres  DECIMAL(2,2) COMMENT 'Basisreserve am Bilanztermin', bonbasres  DECIMAL(2,2) COMMENT 'Bonus-Basisreserve am Bilanztermin', kzschadenrespflege  INTEGER COMMENT 'Kennzeichen fuer die Pflegerenten-Schadenreserve', auffllbilrz  DECIMAL(2,2) COMMENT 'Auffuellung fuer Nachreservierungstyp BILRZ', ausglhbilrz  DECIMAL(2,2) COMMENT 'Bei Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastung durch untersc', ausglsbilrz  DECIMAL(2,2) COMMENT 'Bei Refinanzierung wird hier die Belastung durch die Neubewertung aus unterschiedlichen bi', auffllbilrzkb  DECIMAL(2,2) COMMENT 'Auffuellung fuer Nachreservierungstyp BILRZ kontenbasiert', ausglhbilrzkb  DECIMAL(2,2) COMMENT 'Bei Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastung durch untersc', renueb  DECIMAL(2,2) COMMENT 'Rentenuebertrag', bilbubuebverr  DECIMAL(2,2) COMMENT 'Beitragsuebertrag bezogen auf die Ueberschussverrechnung', kzschadenresdd  INTEGER COMMENT 'Kennzeichen fuer die Dread Disease-Schadenreserve', herderueckstell  DECIMAL(2,2) COMMENT 'Herde-Rueckstellung', gargebverrkto  DECIMAL(2,2) COMMENT 'Summierter Stand aller Verrechnungskonten einer Tarifvariante', zusguthkto  DECIMAL(2,2) COMMENT 'Zusatzguthaben einer Tarifvariante', zusprmkto  DECIMAL(2,2) COMMENT 'Summierter Stand aller Zusatzpraemienkonten einer Tarifvariante', basisuebbrkg1  DECIMAL(2,2) COMMENT 'Einzelvertragliche Basisgroesse gemaess Kommunikationsgroesse 1 der GDV-Ausarbeitung zur B', auffllzinsvaren  DECIMAL(2,2) COMMENT 'Auffuellung fuer Verzinsliche Ansammlung fuer Nachreservierungstyp Zins, aus Rentenbezugsz', auffllerlva  DECIMAL(2,2) COMMENT 'Auffuellung fuer Verzinsliche Ansammlung fuer Nachreservierungstyp Erleben', auffllerlvaren  DECIMAL(2,2) COMMENT 'Auffuellung fuer Verzinsliche Ansammlung fuer Nachreservierungstyp Erleben, aus Rentenbezu', ausglhzinsvaren  DECIMAL(2,2) COMMENT 'Bei Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastung durch die Neu', ausglherlva  DECIMAL(2,2) COMMENT 'Bei Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastung durch die Neu', ausglherlvaren  DECIMAL(2,2) COMMENT 'Bei Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastung durch die Neu', uebzielres  DECIMAL(2,2) COMMENT 'Konto zum Aufsammeln bedingter Ueberschuesse zur Finanzierung der Zielreserve', ausglhbilrzev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier der bereits gegenfinanzierte Teil der Bel', ausglhbilrzkbev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier der bereits gegenfinanzierte Teil der Bel', ausglherlklev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier der bereits gegenfinanzierte Teil der Bel', ausglherlvaev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier der bereits gegenfinanzierte Teil der Bel', ausglherlvarenev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier der bereits gegenfinanzierte Teil der Bel', ausglhzinslklev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier der bereits gegenfinanzierte Teil der Bel', ausglhzinsvaev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier der bereits gegenfinanzierte Teil der Bel', ausglhzinsvarenev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier der bereits gegenfinanzierte Teil der Bel', ausglsbilrzev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier die Belastung durch die Neubewertung aus ', ausglsbilrzkbev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier die Belastung durch die Neubewertung aus ', ausglserlklev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier die Belastung durch die Neubewertung aus ', ausglserlvaev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier die Belastung durch die Neubewertung aus ', ausglserlvarenev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier die Belastung durch die Neubewertung aus ', ausglszinslklev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier die Belastung durch die Neubewertung aus ', ausglszinsvaev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier die Belastung durch die Neubewertung aus ', ausglszinsvarenev  DECIMAL(2,2) COMMENT 'Bei einzelvertraglicher Refinanzierung wird hier die Belastung durch die Neubewertung aus ', ausglhbilrztk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastu', ausglhbilrzkbtk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastu', ausglherlkltk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastu', ausglherlvatk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastu', ausglherlvarentk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastu', ausglhzinslkltk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastu', ausglhzinsvatk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastu', ausglhzinsvarentk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier der bereits gegenfinanzierte Teil der Belastu', ausglsbilrztk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier die Belastung durch die Neubewertung aus unte', ausglsbilrzkbtk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier die Belastung durch die Neubewertung aus unte', ausglserlkltk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier die Belastung durch die Neubewertung aus Erle', ausglserlvatk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier die Belastung durch die Neubewertung aus Erle', ausglserlvarentk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier die Belastung durch die Neubewertung aus Erle', ausglszinslkltk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier die Belastung durch die Neubewertung aus Zins', ausglszinsvatk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier die Belastung durch die Neubewertung aus Zins', ausglszinsvarentk  DECIMAL(2,2) COMMENT 'Bei teilkollektiver Refinanzierung wird hier die Belastung durch die Neubewertung aus Zins', statanzpol  INTEGER COMMENT 'Ausweis in der Statistik fuer die statistische Anzahl der Vertraege (Policen)', auffllraaerl  DECIMAL(2,2) COMMENT 'Reduktion aktivierbarer Ansprueche aus Auffuellung bei Nachreservierung Erleben', auffllraazins  DECIMAL(2,2) COMMENT 'Reduktion aktivierbarer Ansprueche aus Auffuellung bei Nachreservierung Zins (Zinszusatzre', akresrkg  DECIMAL(2,2) COMMENT 'Reserve Abschlusskosten Rueckgewaehr', akresrkgmod  DECIMAL(2,2) COMMENT 'Reserve Abschlusskosten Rueckgewaehr modifiziert mit Stornogewichtung', diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(bearbidabg, lvid, varianteid, vtid) disable novalidate ) COMMENT 'Sammelkontostand Bilanzwerte Kontostaende, die im Rahmen der Beitragszerlegung und bei technischen aenderungen ermittelt und zum Bilanztermin ausgewiesen werden.Historie-streng: Attribute, die zur strengen Historienfuehrung am Vertrag benoetigt werdenSK-' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;