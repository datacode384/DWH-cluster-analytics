CREATE TABLE IF NOT EXISTS  core.ref_tpa_mandant  (tpa_mandant  STRING, mandantid  INTEGER, client  STRING, mandt  STRING)  STORED AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
