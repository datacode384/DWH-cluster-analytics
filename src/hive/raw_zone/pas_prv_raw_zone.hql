CREATE TABLE IF NOT EXISTS  raw_zone.pas_prv (lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', agenturid  INTEGER COMMENT 'id of Prv (Eindeutige Nummer einer Agentur)', kz_prod  INTEGER COMMENT 'Kennzeichnet, ob die Agentur der Produzent ist.', proz_ant  DECIMAL(12,8) COMMENT 'Prozentualer Anteil an einer Groesse.Constraint proz_ant_RANGE: die prozentualen Provision', rollenid  INTEGER COMMENT 'Identifikation einer Partnerrolle in einer Partner-Vertrags-Beziehung', orgeinheit  STRING COMMENT 'Organisationseinheit, der der Vermittler zugeordnet ist (z.B. Regionaldirektion)Organisati', anteilvm2  DECIMAL(9,4) COMMENT 'Anteil an der Provision fuer den zweiten Vermittler', bearbid  INTEGER COMMENT 'the historical version number (logId)', aprov_max  DECIMAL(9,4) COMMENT 'Maximaler Satz der Abschlussprovision (in % der Jahrespraemie)', fprov_max  DECIMAL(9,4) COMMENT 'Maximaler Satz der Folgeprovision (in % der Jahrespraemie)', aprov_relabw  DECIMAL(9,4) COMMENT 'Von der Provisions-Vereinbarung abweichender, vertragsindividueller Promille-Satz der Absc', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', aprov_rel  DECIMAL(9,4) COMMENT 'Promille-Satz der Abschlussprovision.', fprov_rel  DECIMAL(9,4) COMMENT 'Folge-Provisionssatz in Promille.', eprov_rel  DECIMAL(9,4) COMMENT 'Einmalpraemien-Abschluss-Provisionssatz relativ (in Promille).', efprov_rel  DECIMAL(9,4) COMMENT 'Einmalpraemien-Folge-Provisionssatz relativ (in Promille).', mgp_ant  DECIMAL(16,8) COMMENT 'Anteil an der Managmentprovisoion', ausgprov_ant  DECIMAL(16,8) COMMENT 'Anteil an der Ausgabeaufschlags-Provision', partkey  SMALLINT COMMENT 'the partition key', prvid  INTEGER COMMENT 'id of Prv (technischer Zaehler an prv)', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.')  COMMENT 'Die fuer einen Vertrag geltende Vereinbarung mit einem VermittlerHistorie-streng: Attribute, die zur strengen Historienfuehrung am Vertrag benoetigt werdenProvisionssatz: Anteil an verschiedenen ProvisionsartenProvisions_Vereinbarung: Die fuer einen Vert' PARTITIONED BY (process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');