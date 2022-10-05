CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}  (nbsmappingintext_sid  STRING COMMENT 'Surrgate Key', nbsmappingintext_id  STRING COMMENT 'Foreign Key Key table', id_type  INTEGER COMMENT 'id of NbsMappingIntExt (Gibt den Typ an, fuer den das Attribut ID_INT das letzte vergebene', id_int  INTEGER COMMENT 'id of NbsMappingIntExt (Interne numerische ID (z.B. partnerID usw.))', mandantid  INTEGER COMMENT 'Das Field mandantID enthaelt einen Schluesselwert fuer den Mandant, fuer den diese Row gue', id_ext  STRING COMMENT 'Externe alpha-numerische ID', diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(id_int, id_type) disable novalidate ) COMMENT 'Mappingtabelle interne zu externer ID und vv.' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;