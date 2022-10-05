CREATE TABLE IF NOT EXISTS  core.par_but0id (diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', par_but0id_sid  STRING COMMENT 'Surrgate Key', record_hash  INTEGER COMMENT 'Hash for all non technical attributes.', source_system  INTEGER COMMENT 'Name of source system.', par_but0id_id  STRING COMMENT 'Foreign Key', partner  STRING COMMENT 'Geschäftspartnernummer', type  STRING COMMENT 'Identifikationsart', idnumber  STRING COMMENT 'Identifikationsnummer', institute  STRING COMMENT 'Zuständige Institution für die Identifikationsnummer', entry_date  STRING COMMENT 'Datum des Eintrags für die Identifikationsnummer', valid_date_from  STRING COMMENT 'Gültigkeitsbeginn der Identifikationsnummer', valid_date_to  STRING COMMENT 'Gültigkeitsende der Identifikationsnummer', country  STRING COMMENT 'Land, in der eine ID-Nummer gültig ist bzw. vergeben wurde', region  STRING COMMENT 'Region (Bundesstaat, Bundesland, Provinz, Grafschaft)', idnumber_guid  STRING COMMENT 'GUID einer Geschäftspartner Identifikationsnummer', bp_eew_but0id  STRING COMMENT 'Dummyfunktion in der Laenge 1', tpa_mandant STRING, primary key(partner, type) disable novalidate ) PARTITIONED BY (client  STRING COMMENT 'Mandant', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');