CREATE TABLE IF NOT EXISTS  raw_zone.pas_steuwertestand (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbid  INTEGER COMMENT 'the historical version number (logId)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', schichtid  SMALLINT COMMENT 'id of SteuSchicht (Zur Identifizierung der Steuerschicht)', wterm_alt  TIMESTAMP COMMENT 'Wirksamkeitstermin der letzten Meldung an die Steuerschichten', btguebertrag_alt  DECIMAL(16,2) COMMENT 'Beitragsuebertrag der letzten Meldung an die Steuerschichten', wertstand_alt  DECIMAL(16,2) COMMENT 'Summe der Guthaben im Vertrag bei der letzten Meldung an die Steuerschichten', fvm_alt  DECIMAL(16,2) COMMENT 'Fondsvermoegen der letzten Meldung an die Steuerschichten', btggez  DECIMAL(16,2) COMMENT 'gezahlte Beitraege: Summe steuerlicher Beitraege', skestertragalt  DECIMAL(16,2) COMMENT 'Summe ueber Ertraege nach altem KESt-Recht', partkey  SMALLINT COMMENT 'the partition key', wertstand_beginn  DECIMAL(16,2) COMMENT 'Zufluss zu einer Steuerschicht bei Vollnovation', fvmstbel_alt  DECIMAL(15,2) COMMENT 'Fondsvermoegen aus steuerbelasteten Fonds zu Beginn der Bearbeitung', skestertragaltinv  DECIMAL(15,2) COMMENT 'Summe ueber Ertraege im Sinne des Investmentsteuergesetzes', skosteninvsteuer  DECIMAL(15,2) COMMENT 'Summe der Kosten im Sinne des Investmentsteuergesetzes', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Steuerliche Werte werden an der Steuerschicht berechnet. Im Gegensatz zu den Attributen in steuWerteKum sind in steuWerteStand Werte, die nur bei Meldung an die Steuerschicht berechnet werden muessen.' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');