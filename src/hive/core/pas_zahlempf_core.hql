CREATE TABLE IF NOT EXISTS  core.pas_zahlempf (pas_zahlempf_sid  STRING COMMENT 'Surrgate Key', pas_lv_id  STRING COMMENT 'Foreign Key', pas_zahlempf_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', zahlempfid  INTEGER COMMENT 'id of ZahlEmpf (Eindeutige vertragsspezifische Identifizierung eines Zahlungsempfaengers)', antbetrag  DECIMAL(16,2) COMMENT 'Absolutbetrag, den der entsprechende Zahlungsempfaenger erhaelt.', partnerid  INTEGER COMMENT 'Das Field partnerId identifiziert denLeistungsempfaenger, zu dem diese Row eine Aussage ma', bearbid  INTEGER COMMENT 'id of ZahlEmpf (Das Field bearbId identifiziert zusammen mit den Felder lvId und zahlempfI', name  STRING COMMENT 'Nachname einer Person.', vorname  STRING COMMENT 'Vorname des Partners, falls er eine natuerliche Person ist.', strasse  STRING COMMENT 'Bezeichnung fuer eine Strasse, einen Platz, einen Weg etc. incl. der Hausnummer.', landid  INTEGER COMMENT 'Name des jeweiligen Landes.', plz  STRING COMMENT 'Postleitzahl der Strassenadresse', wohnort  STRING COMMENT 'Wohnort der Person', exkwegid  INTEGER COMMENT 'Vertraglich vereinbarter Zahlweg der Leistungen fuer einen Vertragsteil.', blz  STRING COMMENT 'Die BLZ kennzeichnet eine Bank oder auch einzelne Filialen einer Bank. Sie wird z.B. bei u', kontonr  STRING COMMENT 'kontonr', inhalt  STRING COMMENT 'Dieses Attribut beinhaltet einen Freitext variabler Laenge.', zahltermin  TIMESTAMP COMMENT 'Wirksamkeitstermin einer Auszahlung.', auszbetr  DECIMAL(16,2) COMMENT 'Betrag, der bei einem Zahlungsvorgang ausgezahlt wurde', kest  DECIMAL(16,2) COMMENT 'KESt, die bei einem Zahlungsvorgang abgefuehrt wurde', solzu  DECIMAL(16,2) COMMENT 'Solidaritaetszuschlag, der bei einem Zahlungsvorgang abgefuehrt wurde', freista  DECIMAL(16,2) COMMENT 'Pro Zahlungsempfaenger von der Steuer freigestellter Betrag', opausziop  DECIMAL(16,2) COMMENT 'Absolutbetrag der Position offene Posten aus dem Zentralinkasso', hypothdarl  DECIMAL(16,2) COMMENT 'Absolutbetrag eines Hypothekendarlehens', sparzulage  DECIMAL(16,2) COMMENT 'Absolutbetrag einer Sparzulage', anderebetr  DECIMAL(16,2) COMMENT 'Absolutbetrag sonstiger Betraege (z.B. fuer einen Zahlungsempfaenger)', beitrdepot  DECIMAL(16,2) COMMENT 'Betrag aus dem Beitragsdepot (gesamter Wert davon oder Teilbetrag)', ueberzrent  DECIMAL(16,2) COMMENT 'ueberzahlte Renten', vtid  SMALLINT COMMENT 'Das Field vtId identifiziert zusammen mit dem Field lvId denVertragsteil, zu dem diese Row', bankname  STRING COMMENT 'bankname', rueckbtgzfa  DECIMAL(16,2) COMMENT 'Im Field rueckBtgZfA steht der Betrag, der als Saldo in dem dazugehoerigen ""rueckzfa-Kont', kzauszahlung  INTEGER COMMENT 'Kennzeichen Auszahlung', bearbidsto  INTEGER COMMENT 'Wird eine Bearbeitung storniert, wird hier KENN-Bearbeitungsnachweis des stornierenden Vor', stpflke  DECIMAL(16,2) COMMENT 'steuerpflichtiger Kapitalertrag', kistsatz  DECIMAL(16,2) COMMENT 'Kirchensteuersatz, z.B. als Schluesselwerttabelle (aktuell gueltige Saetze: 0,0%; 8,0%; 9,', kist  DECIMAL(16,2) COMMENT 'Kirchensteuer in Euro', relgem  INTEGER COMMENT 'Erhebungsberechtigte Religionsgemeinschaft (Schluesselwerttabelle)', iban  STRING COMMENT 'Fuer Zahlungen ins Ausland muss die IBAN-Nummer angegeben werden koennen.', bic_swift  STRING COMMENT 'Bank Identifier Code oder SWIFT (Society for Worldwide Interbank Financial Telecommunicati', partneridsteupfl  INTEGER COMMENT 'Identifikation der Person, die gegenueber dem Finanzamt steuerpflichtig ist', partneridehegatte  INTEGER COMMENT 'Identifikation des Ehegatten der Person, die gegenueber dem Finanzamt steuerpflichtig ist;', steueridentnummer  STRING COMMENT 'Steueridentifikationsnummer des Steuerpflichtigen', steueridentnreheg  STRING COMMENT 'Steueridentifikationsnummer der Person des Ehegatten des Steuerpflichtigen', namesteupfl  STRING COMMENT 'Nachname des Steuerpflichtigen', vornamesteupfl  STRING COMMENT 'Vorname des Steuerpflichtigen', nameehegatte  STRING COMMENT 'Nachname des Ehegatten des Steuerpflichtigen', vornameehegatte  STRING COMMENT 'Vorname des Ehegatten des Steuerpflichtigen', freistabeanspr  DECIMAL(16,2) COMMENT 'Fuer den Steuerabzug in der aktuellen Abrechnung relevater Kapitalertrag (ohne Abzuege weg', nichtveranlbeschein  INTEGER COMMENT 'Kennzeichen, ob eine Nichtveranlagungsbescheinigung vorliegt', quellekistsatz  INTEGER COMMENT 'Beleg, aus welcher Quelle der Kirchensteuersatz in die Abrechnung uebernommen wurde', kontoid  INTEGER COMMENT 'ID der zugeordneten Bankverbindung zum Partner', quellensteuer  DECIMAL(15,2) COMMENT 'Errechneter Quellensteuerbetrag', kestantanre  DECIMAL(12,8) COMMENT 'Anteil der in kest enthaltenen, anrechenbaren Kapitalertragsteuer. Sie entfaellt auf die a', quellensteuersatz  DECIMAL(15,2) COMMENT 'Verwendeter Quellensteuersatz', steuland  INTEGER COMMENT 'Steuerlicher Wohnsitz des Zahlungsemfaengers', verrechnungssteuer  DECIMAL(15,2) COMMENT 'Hoehe der dieser Auszahlung zugeordneten Verrechnungssteuer', kzsteuervorg  INTEGER COMMENT 'Kennzeichen fuer die Besteuerung der Auszahlung (Kennzeichnet welche Art von Besteuerung a', kzsteuerwiderspruch  INTEGER COMMENT 'Kennzeichen ob der Zahlungsempfaenger Widerspruch gegen die Verrechnungssteuermeldung eing', anschriftid  INTEGER COMMENT 'ID der zugeordneten Anschrift zum Partner', tpa_mandant STRING, wirksam_ab TIMESTAMP, gueltig_ab TIMESTAMP, diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(lvid, zahlempfid) disable novalidate ) COMMENT 'Die Zahlungsempfaenger koennen, muessen jedoch nicht als Partner gefuehrt werden.Zahlungsempfaenger: Die Zahlungsempfaenger koennen, muessen jedoch nicht als Partner gefuehrt werden.' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');