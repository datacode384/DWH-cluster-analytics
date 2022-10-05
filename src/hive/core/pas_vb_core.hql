CREATE TABLE IF NOT EXISTS  core.pas_vb (pas_vb_sid  STRING COMMENT 'Surrgate Key', pas_vb_id  STRING COMMENT 'Foreign Key', pas_vt_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbid  INTEGER COMMENT 'the historical version number (logId)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', vbid  SMALLINT COMMENT 'id of Vb (generically derived from Vb)', varianteid  STRING COMMENT 'Attribut zur eindeutigen Identifizierung einer Tarifvariante. (Contains constants for the ', tbtyp  INTEGER COMMENT 'Eindeutige Identifizierung eines Tarifbaustein-Typs (Stamm, Anpassung, Bonus,...). (Contai', ablbau  TIMESTAMP COMMENT 'Termin zu dem ein Versicherungsbaustein ablaeuft.', tbid  STRING COMMENT 'Eindeutige Identifizierung eines Tarifbausteins', vbbeg  TIMESTAMP COMMENT 'Termin, an dem der LV-Vertragsbaustein technisch wirksam wird.', abl_prber  TIMESTAMP COMMENT 'Ab diesem Zeitpunkt findet keine Berechnung der Provision mehr statt (Initialwert i.d.R. B', zustand  INTEGER COMMENT 'Zustand des Vertragsbausteins, beispielweise beitragspflichtig, leistungspflichtig oder ab', la1  DECIMAL(18,4) COMMENT 'In der Tarifklasse Klassik-Kapital oder Risiko ist la1 die (anfaengliche) Todesfallsumme, ', la2  DECIMAL(16,2) COMMENT 'In der Tarifklasse Klassik-Kapital ist la2 die (abweichende) Erlebensfallsumme, bei Rente ', ratzua  DECIMAL(16,2) COMMENT 'Im Rahmen der Beitragszerlegung ratierlich fuer den Fortschreibungszeitraum ausgewiesener ', rizu  DECIMAL(16,2) COMMENT 'Im Rahmen der Beitragszerlegung ratierlich fuer den Fortschreibungszeitraum ausgewiesener ', rizuogp  DECIMAL(16,2) COMMENT 'Eigenstaendiger Risikozuschlag bei Policierung ohne vorherige Gesundheitspruefung', bssigma  DECIMAL(16,2) COMMENT 'Beitragsbestandteil Sicherheitszuschlag bei Hinterbliebenen-ZV', trdk  DECIMAL(16,2) COMMENT 'Die technische Rundungskorrektur dient dazu, bei der Berechnung des Zahlbeitrags die Addit', tbr  DECIMAL(16,5) COMMENT 'Tarifbeitrag fuer eine tarifspezifische Standardsumme, z.B. 1000,-- DM', tba  DECIMAL(16,2) COMMENT 'Der Beitrag, der auf der jeweiligen Ebene (Vertrag, Vertragsteil) pro Jahr zu entrichten i', tbasum  DECIMAL(16,2) COMMENT 'Enthaelt die voraussichtliche Beitragssumme der Tarifbeitraege fuer den gesamten Zeitraum ', tbak  DECIMAL(16,2) COMMENT 'Der absolute Tarifbeitragsteil fuer nicht gezillmerte einmalige Abschlusskosten ist ein de', tbinkzu  DECIMAL(16,2) COMMENT 'Der absolute Tarifbeitrag eines Versicherungsbausteins kann ausser den ueber Barwertformel', rab  DECIMAL(16,2) COMMENT 'Bei der Berechnung des Zahlbeitrags auf Bausteinebene wird der Beitragsrabatt in dem vorli', nba  DECIMAL(16,2) COMMENT 'Der Normbeitrag ist ein dem Vertragsbaustein zugeordneter absoluter Beitrag, der versicher', nbak  DECIMAL(16,2) COMMENT 'Der absolute Normbeitragsteil fuer nicht gezillmerte einmalige Abschlusskosten ist ein dem', nbinkzu  DECIMAL(16,2) COMMENT 'Der absolute Normbeitrag eines Vertragsbausteins kann ausser den ueber Barwertformeln bere', gba  DECIMAL(16,2) COMMENT 'Der Garantiereservebeitrag ist ein dem Vertragsbaustein zugeordneter absoluter Beitrag, de', zb  DECIMAL(16,2) COMMENT 'Der je Ratenzahlungsabschnitt (tariflich) faellige Zahlbeitrag. Eine Beitragsreduktion aus', bszb  DECIMAL(16,2) COMMENT 'Dieses Attribut enthaelt bei Vertragsbausteinen gegen laufenden Beitrag den jaehrlichen In', zbsum  DECIMAL(16,2) COMMENT 'Enthaelt die voraussichtliche Beitragssumme der Zahlbeitraege fuer den gesamten Zeitraum d', besitzstand  DECIMAL(16,2) COMMENT 'Wird fuer einen Vertrag eine technische aenderung durchgefuehrt, so darf der neue garantie', bstberh  DECIMAL(16,2) COMMENT 'absolute Erhoehung des Tarifbeitrags im Rahmen des Beitragssprungs', bszberh  DECIMAL(16,2) COMMENT 'absolute Erhoehung des Zahlbeitrags im Rahmen des Beitragssprungs', bstbanf  DECIMAL(16,2) COMMENT 'Tarifbeitrag zu Beginn', bszbanf  DECIMAL(16,2) COMMENT 'Zahlbeitrag zu Beginn', bews  DECIMAL(16,2) COMMENT 'Bemessungsgroesse fuer die Kosten.', jb  DECIMAL(16,2) COMMENT 'Juristischer Jahresbeitrag, massgeblich in den AVB', mrs  DECIMAL(16,2) COMMENT 'Mindestrisikosumme errechnet sich als Quotient aus Mindestrisikoleistung (la2) und Mindest', mts  DECIMAL(16,2) COMMENT 'Mindesttodesfallsumme errechnet sich als Quotient aus Mindesttodesfalleistung (la1) und Mi', sra  DECIMAL(16,2) COMMENT 'Bei Altvertraegen: Jaehrlich kalkulierter Beitrag (absolut,vorzeichenbehaftet), der auf Su', gen  SMALLINT COMMENT 'Die Generationsnummer verbindet die VBs einer Schicht, z.B. bei Policierung, Dynamik oder ', bsrdauer  SMALLINT COMMENT 'Rueckgewaehrdauer bei Renten', pb  DECIMAL(16,2) COMMENT 'Provisionsbeitrag', pborig  DECIMAL(16,2) COMMENT 'Anfangsprovisionsbeitrag', pbsprung  DECIMAL(16,2) COMMENT 'Provisionsbeitragssprung', pbsum  DECIMAL(16,2) COMMENT 'Provisionsbeitragssumme-gesamt', bewprov  DECIMAL(16,2) COMMENT 'Bezugsgroesse fuer die Provision', akrabatt  DECIMAL(16,2) COMMENT 'Die im Rahmen der Rueckgabe von Provisionen rabattierten Abschlusskosten', garba  DECIMAL(16,2) COMMENT 'Beitrag der zur Finanzierung von Garantien verwendet wird', diffmonprov  INTEGER COMMENT 'die fuer die Provisionsberechnung relevanten Differenzmonate zu einem Endalter', diffmonabko  INTEGER COMMENT 'die fuer die Abschlusskostenberechnung relevanten Differenzmonate zu einem Endalter', umschlagperiode  INTEGER COMMENT 'Dauer (in Jahren) bis zum Erreichen des ""Umschlagtermins"". Ab dem Umschlagtermin ist das', ablzurech  TIMESTAMP COMMENT 'Ablauf der Versicherungsdauer in Bezug auf Leistungen aus der Zurechnungszeit', minbtg  DECIMAL(16,2) COMMENT 'Vereinbarter Mindestbeitrag', la3  DECIMAL(16,2) COMMENT 'Anfangstodesfallleistung bei Tarifen mit Leistungssprung', minbtgsum  DECIMAL(16,2) COMMENT 'Das Attribut enthaelt die vereinbarte Regelbeitragssumme. Der Wert wird bei der Policierun', lakorr_trk  DECIMAL(16,2) COMMENT 'Leistungskorrektur beim Teilrueckkauf', partkey  SMALLINT COMMENT 'the partition key', zillba  DECIMAL(16,2) COMMENT 'Der Zillmerbeitrag ist ein dem Vertragsbaustein zugeordneter absoluter Beitrag, der versic', nettoba  DECIMAL(16,2) COMMENT 'Der Nettobeitrag ist ein dem Vertragsbaustein zugeordneter absoluter Beitrag, der versiche', kzrefkto2lpfl  SMALLINT COMMENT 'Kennzeichen, ob Auffuellung in refKto2Lpfl() durchgefuehrt ist', varianteidint  STRING COMMENT 'Attribut zur eindeutigen Identifizierung der fuer das SST relevanten internen Tarifvariant', anteilgarniveau  DECIMAL(8,6) COMMENT 'Anteilssatz des Garantieniveaus des Bausteins am Garantieniveau des Vertragsteils', antgarniveauguth  DECIMAL(8,6) COMMENT 'Anteilssatz des Garantieniveaus des Bausteins am Garantieniveau des Guthabens des Vertrags', uebverr  DECIMAL(16,2) COMMENT 'Das Field uebverr enthaelt den Betrag, um den der ratierliche Zahlbeitrag pro Vertragsteil', kuebr  DECIMAL(16,2) COMMENT 'Differenz zwischen konstanter Ueberschussrente und bereits zugeteilter Bonusrente', latodbon  DECIMAL(16,2) COMMENT 'Todesfalleistung Bonus', lasofortbon  DECIMAL(16,2) COMMENT 'Leistung-Sofortbonus (ausser Todesfall)', tbtyporig  INTEGER COMMENT 'Urspruengliche Belegung von tbTyp (Contains the defined segment types for vb.tbTyp. (lf4 s', larbeg  DECIMAL(16,2) COMMENT 'Garantierte Rente plus Bonusrente aus Ueberschussanteilen der Aufschubzeit', rentbbetr  DECIMAL(16,4) COMMENT 'Wert (Hoehe) einer laufenden Bonus-Rente', rentbetr  DECIMAL(16,4) COMMENT 'Wert (Hoehe) einer laufenden Rente', rentgbetr  DECIMAL(16,4) COMMENT 'Wert (Hoehe) einer laufenden Garantie-Rente', rentmbetr  DECIMAL(16,4) COMMENT 'Wert (Hoehe) einer minimalen Garantierente', gargebuehrrel  DECIMAL(10,8) COMMENT 'Die Garantiegebuehr ist eine einmalige oder laufende Gebuehr, die aus dem Beitrag oder dem', gargebuehrrelabw  DECIMAL(10,8) COMMENT 'garGebuehrRelAbw kann durch Vorgabe eines abweichenden Termins garAbwTermin oder direkt be', gueabvariante  TIMESTAMP COMMENT 'Datum, seit dem die Tarifvariante fuer den Baustein wirksam ist', konstuebbeg  TIMESTAMP COMMENT 'Bezugstermin fuer die konstante Ueberschussrente.', labezkonstueb  DECIMAL(16,2) COMMENT 'Bezugsgroesse fuer die konstante Ueberschussrente.', guthabrentueb  DECIMAL(16,2) COMMENT 'Guthaben bei Rentenuebergang fuer Teilkapitalisierung zum Rentenbeginn.', rizuintegr  DECIMAL(16,2) COMMENT 'Differenz des berechneten Zahlbeitrags zu dem Zahlbeitrag ohne Beruecksichtigung der Ueber', tbaorizu  DECIMAL(16,2) COMMENT 'Tarifbeitrag ohne Beruecksichtigung der Uebersterblichkeit', btrdiffmig  DECIMAL(16,2) COMMENT 'Migrationsrabatt am BS', uebverrdiffmig  DECIMAL(16,2) COMMENT 'Verrechnungskorrektur am BS', bewresbar  DECIMAL(16,2) COMMENT 'Barrente aus Bewertungsreserven im Rentenbezug (Jahreswert) am BS', indexnachbearb  INTEGER COMMENT 'Angabe des Indices, durch den die vertragsindividuellen Rechnungsgrundlagen des Bausteins ', erlgar  DECIMAL(16,2) COMMENT 'Erlebensfallgarantie', abschlkovar  INTEGER COMMENT 'Variante der Abschlusskostenmodifikation', steuzuschlag  DECIMAL(15,2) COMMENT 'Steuerzuschlag fuer eine Steuer auf Praemien', gba_nominal  DECIMAL(18,4) COMMENT 'Nominaler Beitrag zur Skalierung von gba', nba_nominal  DECIMAL(18,4) COMMENT 'Nominaler Beitrag zur Skalierung von nba', nettoba_nominal  DECIMAL(18,4) COMMENT 'Nominaler Beitrag zur Skalierung von nettoba', tba_nominal  DECIMAL(18,4) COMMENT 'Nominaler Beitrag zur Skalierung von tba', zillba_nominal  DECIMAL(18,4) COMMENT 'Nominaler Beitrag zur Skalierung von zillba', akzkalk  DECIMAL(15,2) COMMENT 'Volumen der klassischen gezillmerten kalkulatorischen AK', aknzkalk  DECIMAL(15,2) COMMENT 'Volumen der klassischen nicht gezillmerten kalkulatorischen AK', akzbil  DECIMAL(15,2) COMMENT 'Volumen der klassischen gezillmerten bilanziellen AK', aknzbil  DECIMAL(15,2) COMMENT 'Volumen der klassischen nicht gezillmerten bilanziellen AK', kzzielreserve  INTEGER COMMENT 'Kennzeichen zur Verwendung der Ueberschuesse fuer Zielreserve', bszb_nominal  DECIMAL(18,4) COMMENT 'Nominaler Beitrag zur Skalierung von bszb.', garantieniveau  DECIMAL(9,6) COMMENT 'Niveau der Beitragsgarantie', garantiefinanzierungstyp  INTEGER COMMENT 'Typ der Finanzierung der Garantie, etwa durch Sparbeitraege (Unique identifier of the meth', steuzuschlagsum  DECIMAL(15,2) COMMENT 'Summe der voraussichtlich insgesamt zu entrichtenden Steuer auf Praemien fuer die gesamte ', c_alttarifid  STRING COMMENT 'Alte Tarifbezeichnung fuer ERO-Tarife bei Migration', c_gammamig  DECIMAL(12,8) COMMENT 'Zusaetzliche Verwaltungskosten aufgrund Deckungskapitaldifferenz bei Migration', c_la1_akt  DECIMAL(16,2) COMMENT 'Aktuelle Leistung la1 am Vertragsbaustein', c_gammamigren  DECIMAL(12,8) COMMENT 'Zusaetzliche Verwaltungskosten aufgrund Deckungskapitaldifferenz fuer die leistungspflicht', c_kzlstspektrum  INTEGER COMMENT 'Kennzeichen, ob der Baustein dem Leistungsprofil des VT folgt.', c_gammamigbfr  DECIMAL(12,8) COMMENT 'Zusaetzliche Verwaltungskosten aufgrund Deckungskapitaldifferenz fuer die beitragsfreie Ze', c_tba_ungezillmert  DECIMAL(16,2) COMMENT 'Der Tarifbeitrag ohne Beruecksichtung der gezillmerten Abschlusskosten.', c_tba_zstrich  DECIMAL(16,2) COMMENT 'Der Tarifbeitrag verschoben um ein Jahr fuer angepasste Abschlusskosten.', c_la2_akt  DECIMAL(16,2) COMMENT 'Aktuelle Leistung la2 am Vertragsbaustein', c_ak_rizu  DECIMAL(16,2) COMMENT 'Abschlusskosten auf Risikozuschlag absolut', c_fba  DECIMAL(16,2) COMMENT 'Fiktiver Beitrag als Bezugsgroesse fuer den Grundueberschuss', c_bezgrundueb  DECIMAL(16,2) COMMENT 'Bezugsgroesse fuer den Grundueberschuss', c_bezzinsueb  DECIMAL(16,2) COMMENT 'Bezugsgroesse fuer den Zinsueberschuss', c_dauzusleist  DECIMAL(16,10) COMMENT 'Zusatzleistungsdauer', c_larisueb  DECIMAL(16,2) COMMENT 'Bonusleistung aus Risikoueberschuessen', c_antfakzinsueb  DECIMAL(12,10) COMMENT 'Anteilsfaktor Bonusleistung / Ansammlungsguthaben aus Zinsueberschuessen im Verhaeltnis zu', c_antfakbezgrundueb  DECIMAL(12,10) COMMENT 'Anteilsfaktor auf die Bezugsgroesse fuer den Grundueberschuss', c_lagarbon  DECIMAL(16,2) COMMENT 'Garantiebonusleistung', c_tbagarbon  DECIMAL(16,2) COMMENT 'Garantiebonusbeitrag', c_antfaksue  DECIMAL(16,10) COMMENT 'Anteilsfaktor auf die Bemessungsgroesse fuer den Schlussueberschuss', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid, vtid, vbid) disable novalidate ) COMMENT 'vb' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');