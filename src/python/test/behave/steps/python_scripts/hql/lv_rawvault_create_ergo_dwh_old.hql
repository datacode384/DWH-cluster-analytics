CREATE TABLE IF NOT EXISTS ${hiveconf:database}.${hiveconf:table_name}  (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', pdid  STRING COMMENT 'Eindeutige Identifikation des Produkts, das dem LV-Vertrag zugeordnet ist.', generation  STRING COMMENT 'Kennzeichnet die Generationenfolge grundsaetzlich identischer Produkte oder Tarife im Zeit', mandantid  INTEGER COMMENT 'Das Field mandantId enthaelt einen Schluesselwert fuer den Mandant, fuer den diese Row gue', verzinsbeginn  TIMESTAMP COMMENT 'Termin, ab dem das Beitragsdepotkonto verzinst wird.', zweiink  INTEGER COMMENT 'Eindeutige Identifizierung einer Beitragszahlweise.', zinsbdep  DECIMAL(8,8) COMMENT 'Hoehe des fuer das Beitragsdepot festgelegten Zinssatzes in Prozent.', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', guebiszins  TIMESTAMP COMMENT 'Datum, bis zu dem der fest vorgegebene Zinssatz am Beitragsdepot gueltig ist.', betrag  DECIMAL(2,2) COMMENT 'Betrag, der in das Beitragsdepot eingezahlt oder ihm entnommen wird.', waehrungid  INTEGER COMMENT 'Das Attribut dient zur eindeutigen Identifizierung einer Waehrung', lvbegt  TIMESTAMP COMMENT 'Erster technischer Beginn des Vertrages.', lvablt  TIMESTAMP COMMENT 'Ablauftermin des Vertrags', trdk  DECIMAL(2,2) COMMENT 'Die technische Rundungskorrektur dient dazu, bei der Berechnung des Zahlbeitrags die Addit', btrdiffmig  DECIMAL(2,2) COMMENT 'Wird im Rahmen einer Migration als Differenz aus bisherigem und errechnetem Beitrag (gemae', btrabgl  DECIMAL(2,2) COMMENT 'Dient zum Abgleich zwischen vorgegebenem und errechnetem Beitrag; wird gemaess Beitragsfae', vstkk  DECIMAL(2,2) COMMENT 'In den Beitrag eingerechnete Stueckkosten, die bei laufender Beitragszahlung pro Ratenzahl', uebverr  DECIMAL(2,2) COMMENT 'Der laufende, vorschuessig zugeteilte ueberschuss von Vertragsteilen kann zur Verrechnung ', lvjahrtag  TIMESTAMP COMMENT 'Hier wird der Jahrestag der Versicherung gefuehrt.', lvstatus  INTEGER COMMENT 'Status, in dem sich ein Antrag bzw. Vertrag befindet', zb  DECIMAL(2,2) COMMENT 'Der je Ratenzahlungsabschnitt (tariflich) faellige Zahlbeitrag. Eine Beitragsreduktion aus', vzborig  DECIMAL(2,2) COMMENT 'Brutto-Zahlbeitrag gemaess Zahlweise (historischer Stand, Merkposten)', kzvorg  INTEGER COMMENT 'Bestimmt die Qualitaet der Vorgabe: z.B. Vorgabe der Leistung, des Beitrags oder der Ablau', kz_anr  INTEGER COMMENT 'Kennzeichen, ob anrechenbare Werte angegeben werden sollen.', anrewert  DECIMAL(2,2) COMMENT 'Anrechenbarer Wert aus einem Vorvertrag', annahmeart  SMALLINT COMMENT 'Art der Annahme: Zur Zeit nur Unterteilung in Annahme mit oder ohne Gesundheitspruefung', fr_switch  INTEGER COMMENT 'Gibt die Anzahl der restlichen gebuehrenfreien Switchvorgaenge an.', let_switch  TIMESTAMP COMMENT 'Datum des letzten Switchen.', kz_versst  INTEGER COMMENT 'Gibt an, ob eine Versicherungssteuer zu bezahlen ist oder nicht.', versst  DECIMAL(2,2) COMMENT 'Versicherungssteuer in Landeswaehrung.', rentbekz  INTEGER COMMENT 'Kennzeichen, ob eine Rente (tatsaechlich) bezogen wird', vertriebsweg  INTEGER COMMENT 'Vertriebsweg', versstnate  DECIMAL(2,2) COMMENT 'Stempelsteuerbetrag auf Naturaleinlagebetrag', errbtgniv  DECIMAL(2,2) COMMENT 'Erreichtes Beitragsniveau', minbtg  DECIMAL(2,2) COMMENT 'Vereinbarter Mindestbeitrag', zweivorza  INTEGER COMMENT 'Zahlweise fuer die Zinsen der Vorauszahlung', btrdiffwsw  DECIMAL(2,2) COMMENT 'Beitragsdifferenz aus einer Waehrungsumstellung', kznachz  INTEGER COMMENT 'Kennzeichen bzgl. Nachzahlungsvariante fuer Altersvorsorgevertrag', uebverrnsp  DECIMAL(2,2) COMMENT 'Uebersch-Verrechnung-rat nach Beitragssprung', kzanbieterwechsel  INTEGER COMMENT 'Kennzeichen, ob ein Anbieterwechsel vorliegt.', kzrelease  INTEGER COMMENT 'Releasekennzeichen', antragssteuerung  INTEGER COMMENT 'Kennzeichen fuer die Steuerung des Antragsprozesses', partkey  SMALLINT COMMENT 'the partition key', kzangvers  INTEGER COMMENT 'Kennzeichen, ob bei Antrag Speichern Angebotsbriefe versendet wurden.', kzauszstop  INTEGER COMMENT 'Kennzeichen, ob das Guthaben auf dem Abklaerungskonto ausgezahlt werden kann (0) oder es e', kzabfindungkbr  INTEGER COMMENT 'Wird gesetzt, wenn im Geschaeftsprozess Vorbereitung Verrentung Riester die Abfindung Klei', kollnr  STRING COMMENT 'Kollektiv-Nummer', lfdkollnr  STRING COMMENT 'Laufende Nummer als Ergaenzung zur Kollektiv-Nummer', verwgrpnr  STRING COMMENT 'Verwaltungsgruppennummer', musterid  STRING COMMENT 'Muster-Kennung', zuzahlpol  DECIMAL(2,2) COMMENT 'Zuzahlung bei Policierung', isvoraussschaedverw  SMALLINT COMMENT 'Gibt an, ob die fachliche Voraussetzung fuer schaedliche Verwendung gemaess gegeben ist (0', kistamwirkdat  TIMESTAMP COMMENT 'Wirksamkeitstermin des letzten Leistungsgevos zu dem in der Simulation Ablauf die Schnitts', kistambearbdat  TIMESTAMP COMMENT 'Bearbeitungsdatum der letzten Simulation Ablauf, bei der die Schnittstelle EKTC01 (fuer ei', kzleistungsarttod  INTEGER COMMENT 'Kennzeichnung, ob bei Tod kapitalisiert werden oder die Zeitrente berechnet werden soll', steulandsekundaer  INTEGER COMMENT 'Von steuLandPrimaer abweichendes Land, fuer den der Vertrag aufgrund des Wohnsitzes des VN', steuzuschlagzuzahl  DECIMAL(2,2) COMMENT 'Steuerzuschlag auf Zuzahlungen, summiert die Steuerzuschlaege auf Praemien der Zuzahlungs-', steuzuschlag  DECIMAL(2,2) COMMENT 'Steuerzuschlag der auslaendischen Versicherungssteuer. Summiert die Steuerzuschlaege der V', steulandprimaer  INTEGER COMMENT 'Angabe des Landes, fuer welches der Vertrag aufgrund des abschliessenden VU primaer steuer', kzrechenkern  INTEGER COMMENT 'Kennzeichen, ob der Vertrag mit TBF=1 oder LP=2 angelegt wurde (The sign of the account mo', vorgperformance  DECIMAL(8,8) COMMENT 'Durch den Sachbearbeiter erfasste Vorgabe fuer die individuelle Modellrechnung.', c_werbhilf  INTEGER COMMENT 'Schluesselfeld mit dem erfasst wird, mit welcher Verkaufsmethode der Vertrieb den Vertrag ', c_zugweg  INTEGER COMMENT 'Schluesselfeld mit dem erfasst wird, ueber welchen Zugangsweg bzw. Vertriebspfad ein Vertr', c_wikmgl  INTEGER COMMENT 'Kennzeichnet ob der Vertrag wieder in Kraft gesetzt werden darf', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Die Rows in Table lv repraesentieren einen oder mehrere Vertragsstaende; temporal identifiziert durch das Field lvId. Die Vertragsstaende sind von 1 beginnend durchnummeriert, wobei es auch Luecken geben darf. Welche Vertragsstaende eine Row repraesentie' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;
