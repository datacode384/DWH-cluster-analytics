CREATE TABLE IF NOT EXISTS  raw_zone.pas_zahlempfdat (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', zahlempfid  INTEGER COMMENT 'id of ZahlEmpfDat (Eindeutige vertragsspezifische Identifizierung eines Zahlungsempfaenger', antprozent  DECIMAL(16,2) COMMENT 'Anteil des Auszahlunsgbetrags in Prozent, den der entsprechende Zahlungsempfaenger erhaelt', bankname  STRING COMMENT 'bankname', bic_swift  STRING COMMENT 'Bank Identifier Code oder SWIFT (Society for Worldwide Interbank Financial Telecommunicati', blz  STRING COMMENT 'Die BLZ kennzeichnet eine Bank oder auch einzelne Filialen einer Bank. Sie wird z.B. bei U', exkwegid  INTEGER COMMENT 'Vertraglich vereinbarter Zahlweg der Leistungen fuer einen Vertragsteil.', freista  DECIMAL(16,2) COMMENT 'Pro Zahlungsempfaenger von der Steuer freigestellter Betrag', iban  STRING COMMENT 'Fuer Zahlungen ins Ausland muss die IBAN-Nummer angegeben werden koennen.', inhalt  STRING COMMENT 'Dieses Attribut beinhaltet einen Freitext variabler Laenge.', kest_abzug  INTEGER COMMENT 'Kennzeichen, ob bei diesem Zahlungsempfaenger die KESt abzuziehen ist. (Ist dann relevant,', kistsatz  DECIMAL(16,2) COMMENT 'Kirchensteuersatz, z.B. als Schluesselwerttabelle (aktuell gueltige Saetze: 0,0%; 8,0%; 9,', kontonr  STRING COMMENT 'kontonr', kzauszahlung  INTEGER COMMENT 'Kennzeichen Auszahlung', landid  INTEGER COMMENT 'Name des jeweiligen Landes.', bearbid  INTEGER COMMENT 'the historical version number (logId)', name  STRING COMMENT 'Nachname einer Person.', partnerid  INTEGER COMMENT 'Das Field partnerId identifiziert den Leistungsempfaenger, zu dem diese Row eine Aussage m', plz  STRING COMMENT 'Postleitzahl der Strassenadresse', relgem  INTEGER COMMENT 'Erhebungsberechtigte Religionsgemeinschaft (Schluesselwerttabelle)', strasse  STRING COMMENT 'Bezeichnung fuer eine Strasse, einen Platz, einen Weg etc. incl. der Hausnummer.', vorname  STRING COMMENT 'Vorname des Partners, falls er eine natuerliche Person ist.', wohnort  STRING COMMENT 'Wohnort der Person', planschrittid  INTEGER COMMENT 'Arbeitsschritt, fuer den die Zahlungsempfaengerdaten planweise erfasst worden sind Hinweis', betrag  DECIMAL(16,2) COMMENT 'Auszahlunsgbetrag den der entsprechende Zahlungsempfaenger erhaelt.', kzrestbetrag  INTEGER COMMENT 'Kennzeichen, dass dieser Zahlungsempfaenger den Restbetrag erhaelt.( Ist dann relevant, we', zahltermin  TIMESTAMP COMMENT 'Wirksamkeitstermin einer Auszahlung', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', partneridsteupfl  INTEGER COMMENT 'Identifikation der Person, die gegenueber dem Finanzamt steuerpflichtig ist', partneridehegatte  INTEGER COMMENT 'Identifikation des Ehegatten der Person, die gegenueber dem Finanzamt steuerpflichtig ist;', steueridentnummer  STRING COMMENT 'Steueridentifikationsnummer des Steuerpflichtigen', steueridentnreheg  STRING COMMENT 'Steueridentifikationsnummer der Person des Ehegatten des Steuerpflichtigen', namesteupfl  STRING COMMENT 'Nachname des Steuerpflichtigen', vornamesteupfl  STRING COMMENT 'Vorname des Steuerpflichtigen', nameehegatte  STRING COMMENT 'Nachname des Ehegatten des Steuerpflichtigen', vornameehegatte  STRING COMMENT 'Vorname des Ehegatten des Steuerpflichtigen', freistabeanspr  DECIMAL(16,2) COMMENT 'Fuer den Steuerabzug in der aktuellen Abrechnung relevanter Kapitalertrag (ohne Abzuege we', nichtveranlbeschein  INTEGER COMMENT 'Kennzeichen, ob eine Nichtveranlagungsbescheinigung vorliegt', quellekistsatz  INTEGER COMMENT 'Beleg, aus welcher Quelle der Kirchensteuersatz in die Abrechnung uebernommen wurde', bearbidrefabr  INTEGER COMMENT 'BearbId der Abrechnung, zu der dieser Satz abgerechnet wurde. Wird geschrieben von der aut', kontoid  INTEGER COMMENT 'ID der zugeordneten Bankverbindung zum Partner', kestantanre  DECIMAL(12,8) COMMENT 'Anteil der in kest enthaltenen, anrechenbaren Kapitalertragsteuer. Sie entfaellt auf die a', steuland  INTEGER COMMENT 'Steuerlicher Wohnsitz des Zahlungsemfaengers', kzsteuervorg  INTEGER COMMENT 'Kennzeichen fuer die Besteuerung der Auszahlung (Kennzeichnet welche Art von Besteuerung a', kzsteuerwiderspruch  INTEGER COMMENT 'Kennzeichen ob der Zahlungsempfaenger Widerspruch gegen die Verrechnungssteuermeldung eing', anschriftid  INTEGER COMMENT 'ID der zugeordneten Anschrift zum Partner', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Fuer die Speicherung der Zahlungsempfaengerdaten wird eine neue juristische Tabelle ""Zahlungsempfaengerdaten"" (kurz: zahlEmpfDat) benoetigt. Die Ablage der Daten in zwei Tabellen ist durch die erforderliche Trennung der Zahlungsempfaengerdaten von den ' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');