CREATE TABLE IF NOT EXISTS  core.pas_hvgruppe (pas_hvgruppe_sid  STRING COMMENT 'Surrgate Key', pas_hvgruppe_id  STRING COMMENT 'Foreign Key', pas_lv_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', vtid  SMALLINT COMMENT 'Das Field vtId identifiziert zusammen mit dem Field lvId denVertragsteil, zu dem diese Row', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', hvgruppeid  SMALLINT COMMENT 'id of HvGruppe (generically derived from HvGruppe)', zb  DECIMAL(16,2) COMMENT 'Der je Ratenzahlungsabschnitt (tariflich) faellige Zahlbeitrag. Eine Beitragsreduktion aus', minbtg  DECIMAL(16,2) COMMENT 'Vereinbarter Mindestbeitrag', uebverr  DECIMAL(16,2) COMMENT 'Der laufende, vorschuessig zugeteilte ueberschuss von Vertragsteilen kann zur Verrechnung ', btrabgl  DECIMAL(16,2) COMMENT 'Dient zum Abgleich zwischen vorgegebenem und errechnetem Beitrag; wird gemaess Beitragsfae', vstkk  DECIMAL(16,2) COMMENT 'In den Beitrag eingerechnete Stueckkosten, die bei laufender Beitragszahlung pro Ratenzahl', trdk  DECIMAL(16,2) COMMENT 'Die technische Rundungskorrektur dient dazu, bei der Berechnung des Zahlbeitrags die Addit', zweiink  INTEGER COMMENT 'Eindeutige Identifizierung einer Beitragszahlweise.', kzvorg  SMALLINT COMMENT 'Bestimmt die Qualitaet der Vorgabe: z.B. Vorgabe der Leistung, des Beitrags oder der Ablau', hvtypid  INTEGER COMMENT 'Hauptversicherungsgruppentyp', zustand  INTEGER COMMENT 'Es gibt keine fachlich sinnvolle Aggregation von Vertragsteil-Zustaenden auf Vertrags-Zust', hauptfaelligkeit  TIMESTAMP COMMENT 'Hauptfaelligkeit - wird produktabhaengig bestimmt - enthaelt das Jahr des Versicherungsbeg', kzteilnovation  INTEGER COMMENT 'Kennzeichen, das den Tatbestand der ""Teilnovation"" anzeigt', maxstoabsch  DECIMAL(16,2) COMMENT 'Maximaler Stornoabschlag, der abgezogen werden kann. Kann durch den SB manuell vorgegeben ', partkey  SMALLINT COMMENT 'the partition key', kzgrundzulage  INTEGER COMMENT 'Kennzeichen, ob Grundzulage eingerechnet werden soll oder nicht: 0 - Nein, 1 - Ja', pricingid  INTEGER COMMENT 'Pricing-Variante: globales Attribut zur Festlegung des Pricings (z.B. Haustarif)', beg_btgpause  TIMESTAMP COMMENT 'Beginn der Beitragspause', end_btgpause  TIMESTAMP COMMENT 'Ende der Beitragspause', vzb_vorpause  DECIMAL(16,2) COMMENT 'Beitrag vor der Beitragspause', kzwohnfoerdkto  INTEGER COMMENT 'Nach Entnahme fuer Wohnriester (unschaedlich) ist ein Wohnfoerderkonto anzulegen. Auch wen', uebwohnfoerdk  INTEGER COMMENT 'Information des VN, wohin das Wohnfoerderkonto uebertragen werden soll. Auswahlbox-Belegun', verstke  DECIMAL(16,2) COMMENT 'Betrag des steuerpflichtigen Kapitalertrages aus abgegangenen Steuerschichten, der bereits', einwilldatum  TIMESTAMP COMMENT 'Datum wann der VN seine Einwilligung zur Uebermittlung der Daten erteilt bzw. widerrufen h', vorgabewert  DECIMAL(16,2) COMMENT 'Vorgegebener Wert fuer die SST - Berechnung.', verrenttfl  DECIMAL(16,2) COMMENT 'Zu verrentende Todesfallleistung', zbanteilbeginn  DECIMAL(16,2) COMMENT 'Der anteilig faellige Zahlbeitrag zu Beginn des Vertragsteils, wenn der Zeitraum von VT-Be', zbanteilende  DECIMAL(16,2) COMMENT 'Der anteilig faellige, letzte Zahlbeitrag des Vertragsteils vor Ablauf der Beitragszahlung', tpid  STRING COMMENT 'Das Field tpId identifiziert das Teilprodukt, zu dem die HvGruppe zugeordnet werden kann.', vorg_btgoptverl  DECIMAL(16,2) COMMENT 'Vorgabewert fuer die Neukalkulation im Rahmen der optionalen Verlaengerung', gehzulberech  DECIMAL(16,2) COMMENT 'Sozialversicherungspflichtiges Vorjahresgehalt auf dessen Basis in der Modellrechnung der ', gehzulbersteig  DECIMAL(16,8) COMMENT 'Jaehrlicher Steigerungssatz fuer GehaltZulagenBerech', gehzulberbasjahr  SMALLINT COMMENT 'Jahr auf das sich GehaltZulagenBerech bezieht', kzberufeinstbonus  INTEGER COMMENT 'Kennzeichen, ob der Berufseinsteigerbonus beruecksichtigt werden soll', kzbtganpassung  INTEGER COMMENT 'Kennzeichen, ob bzw. nach welcher Regel eine Beitragsanpassung in der Modellrechnung erfol', crkid  INTEGER COMMENT 'Chance-Risiko-Klasse gemaess AltZertG', kzzulagebervn  INTEGER COMMENT 'Kennzeichnung, ob der Versicherungsnehmer mittelbar oder unmittelbar zulageberechtigt ist', kzzulagebereheg  INTEGER COMMENT 'Kennzeichnung, ob der Ehegatte mittelbar oder unmittelbar zulageberechtigt ist', eigenbtgeheg  DECIMAL(16,2) COMMENT 'Eigenbeitrag des (unmittelbar zulageberechtigten) Ehegatten, auf dessen Basis in der Model', eigenbtgbisjahr  SMALLINT COMMENT 'Jahr, bis zu dem der Eigenbeitrag des (unmittelbar zulageberechtigten) Ehegatten bezahlt w', eigenbtgehegabw  DECIMAL(15,2) COMMENT 'Abweichender Eigenbeitrag des Ehegatten in einem Jahr', eigenbtgabwjahr  SMALLINT COMMENT 'Jahr des abweichenden Eigenbeitrags des Ehegatten', btgprofil  INTEGER COMMENT 'Kennzeichen zur Identifizierung eines Beitragsprofils', btgueberz  DECIMAL(15,2) COMMENT 'ueberzahlte Beitraege. Werden erfasst in Uebertragung Rueckkauf im Rahmen von Anbieterwech', btgrueckstand  DECIMAL(15,2) COMMENT 'rueckstaendige / offene Beitraege. Werden erfasst in Uebertragung Rueckkauf im Rahmen von ', tpvarpiaid  INTEGER COMMENT '(Teil)Produktvariante zur CRK-Klassifizierung durch Produktinformationsstelle Altersvorsor', kzzuzgvb  INTEGER COMMENT 'Kennzeichen, ob eine Zuzahlung als Geringverdienerbeitrag (Gvb) zur Erlangung des bAV-Foer', kzfoerdergruppierung  INTEGER COMMENT 'relevante Foerdergruppierung zur Ermittlung des zulaessigen Maximalbeitrags (relevante Foe', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid, hvgruppeid) disable novalidate ) COMMENT 'Im Rahmen der bAV koennen unterschiedliche Zahlweisen fuer Arbeitgeber (AG) und Arbeitnehmer (AN) vorgegeben werden. Die notwendigen Informationen werden pro HV bzw. in einer hvGruppe gefuehrt, die die eigentliche HV und die zugehoerigen ZV zusammenfas' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');