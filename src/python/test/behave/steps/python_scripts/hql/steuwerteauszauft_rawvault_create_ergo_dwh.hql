CREATE TABLE IF NOT EXISTS ${hiveconf:database}.${hiveconf:table_name}  (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', schichtid  SMALLINT COMMENT 'id of SteuWerteAuszAuft (Steuerschicht)', beitragstyp  INTEGER COMMENT 'id of SteuWerteAuszAuft (Auszahlbetrag des Beitragstyps)', bearbidref  INTEGER COMMENT 'id of SteuWerteAuszAuft (Das Field bearbIdRef identifiziert die Bearbeitung, d.h. die Teil', partkey  SMALLINT COMMENT 'the partition key', bearbid  INTEGER COMMENT 'the historical version number (logId)', ausz  DECIMAL(2,2) COMMENT 'Auszahlbetrag des Beitragstyps', kzsteuerabzugbav  INTEGER COMMENT 'Kennzeichen, ob die Auszahlung bescheinigt wurde (Auspraegung = ""nein"") oder dem Steuera', wertvorausz  DECIMAL(2,2) COMMENT 'Wertstand vor Auszahlung (die Auszahlung erfolgte aus diesem Vertragsvermoegen)', wterm  TIMESTAMP COMMENT 'Wirksamkeitstermin der Auszahlung', beschke  DECIMAL(2,2) COMMENT 'Gesamter fuer die Auszahlung bescheinigter Kapitalertrag (unter Beruecksichtigung von Steu', antkestertrag2005  DECIMAL(2,2) COMMENT 'Gesamter ausgezahlter Ertrag (Differenzmethode ab 2005) des Beitragstyps', antkestertragalt  DECIMAL(2,2) COMMENT 'Gesamte ausgezahlte Zinsen (Ertragsbegriff vor 2005) des Beitragstyps', antfondsertrag2005  DECIMAL(2,2) COMMENT 'In antKestErtrag2005 enthaltener Ertrag, der aus der Anlage in steuerbelasteten Publikumsf', btgsteu  DECIMAL(2,2) COMMENT 'Absoluter Beitragsanteil, auf den sich die Auszahlung bezieht', nachsteuer  DECIMAL(2,2) COMMENT 'Steuerbetrag aus der Nachversteuerung von Praemien, die von der angegebenen Auszahlung abg', entnprozsatzstpfl  DECIMAL(8,8) COMMENT 'Gibt an, wieviel Prozent des steuerpflichtigen Vermoegens mit dieser Auszahlung entnommen ', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Aufgeteilte Steuerwerte fuer jede Auszahlung' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;