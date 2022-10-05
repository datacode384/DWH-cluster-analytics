CREATE TABLE IF NOT EXISTS  raw_zone.pas_pd (productid  STRING, generation  STRING, pdidext  STRING, pdklasse  STRING, gapro  TIMESTAMP, gbpro  TIMESTAMP, waehrungid  STRING, kz_getr_veranl  STRING, mandantid  STRING, kzhochrechnung  INTEGER, kzrrg  STRING, kzpf  STRING, modelid  STRING, kzruerup  STRING, modellbrid  STRING, fo_erm_kbr  STRING, unschaerfefak  DOUBLE, reginvestzuzahl  STRING, kzverrentung  STRING, fo_kaljwerte  INTEGER, fo_kundiwerte  INTEGER, produktklasse  STRING, kzbtgmapping  STRING, kzzulageverlauf  STRING, fo_inforbegwerte  INTEGER, fo_belegelstspglhvg  INTEGER, kzkapmarktabh  INTEGER, fo_gebuehr_grund  INTEGER, fo_gebuehr_erheb  INTEGER, kzbtgprofil  STRING, customer  STRING, insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');