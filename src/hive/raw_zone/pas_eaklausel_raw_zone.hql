CREATE TABLE IF NOT EXISTS  raw_zone.pas_eaklausel (partnerid  INTEGER COMMENT 'id of VpVT (Das Field partnerId identifiziert die versicherte Person, zu der diese Row ein', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', rollenid  INTEGER COMMENT 'id of VpVT (Identifikation einer Partnerrolle in einer Partner-Vertrags-BeziehungConstrain', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', lfdnr  INTEGER COMMENT 'id of EaKlausel (In diesem Attribut wird eine ""laufende Nummer"" (natuerliche Zahl) gefue', klauselid  INTEGER COMMENT 'Eindeutige Identifikation einer (verschluesselbaren) Standard-Klausel zur Definition eines', klauseltext  STRING COMMENT 'Genaue Spezifikation einer E/A-Klausel, z.B. Angabe einer Erkrankung oder Funktionseinschr', partkey  SMALLINT COMMENT 'the partition key', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Ein-/Ausschluss-Klausel: Es gibt Aus- bzw. Einschluesse aufgrund medizinischer, beruflicher und sportlicher Risiken. Einschlussklausel bedeutet, dass ein bestimmtes Risiko explizit eingeschlossen wird. Fuer eine Ausschlussklausel gilt die analoge Aussage' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');