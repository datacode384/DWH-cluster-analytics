CREATE TABLE IF NOT EXISTS  ${hiveconf:database}.${hiveconf:table_name}  (skquotausgl_sid  STRING COMMENT 'Surrgate Key', skquotausgl_id  STRING COMMENT 'Foreign Key Key table', bearbnw_sid  STRING COMMENT 'Foreign Key', lv_sid  STRING COMMENT 'Foreign Key', vt_sid  STRING COMMENT 'Foreign Key', bearbnw_id  STRING COMMENT 'Foreign Key', lv_id  STRING COMMENT 'Foreign Key', vt_id  STRING COMMENT 'Foreign Key', lvid  STRING COMMENT 'id of Lv (Das Field lvId identifiziert denVertrag, zu dem diese Row gehoert.)', bearbid  INTEGER COMMENT 'the historical version number (logId)', bearbidabg  INTEGER COMMENT 'the historical version that invalidates this version (nextLogId)', vtid  SMALLINT COMMENT 'id of Vt (generically derived from Vt)', konspartid  INTEGER COMMENT 'id of SkQuotAusgl (Das Field konsPartID identifiziert den Konsortialpartner.)', varianteid  STRING COMMENT 'id of SkQuotAusgl (Attribut zur eindeutigen Identifizierung einer Tarifvariante.)', bearbdat  TIMESTAMP COMMENT 'Termin, an dem das Objekt bearbeitet wurde.', bearbdatsto  TIMESTAMP COMMENT 'Bearbeitungstermin des stornierenden Vorgangs', feindat  SMALLINT COMMENT 'Feinsteuerungsinformation zum Termin Termin-Wirksamkeit', konsortid  INTEGER COMMENT 'identifiziert den Konsortialpartner', kzeinmalbtrg  INTEGER COMMENT 'Kennzeichen, ob Einmalbeitrag vorliegt oder nicht.', kzkoll  SMALLINT COMMENT 'Ordnet eine Produkt-/Tarif-Kombination dem Kollektiv- oder Einzelbereich zu', mandantid  INTEGER COMMENT 'Das Field mandantID enthaelt einen Schluesselwert fuer den Mandant, fuer den diese Row gue', pdid  STRING COMMENT 'Eindeutige Identifikation des Produkts, das dem LV-Vertrag zugeordnet ist.', quotausglproperty  INTEGER COMMENT 'id of SkQuotAusgl (Schluesselverzeichnis fuer quotalen Ausgleich)', quotausglvalue  DECIMAL(2,2) COMMENT 'Wert des quotalen Ausgleichs in Waehrungseinheiten', sex  INTEGER COMMENT 'Identifizierender Schluessel fuer das Geschlecht einer natuerlichen Person.', tfid  STRING COMMENT 'Eindeutiger Identifikator eines LV-Tarifs', waehrungid  INTEGER COMMENT 'Das Attribut dient zur eindeutigen Identifizierung einer Waehrung', wirkdat  TIMESTAMP COMMENT 'Termin, zu dem eine Aenderung wirksam wird.', zeichjahr  TIMESTAMP COMMENT 'Zeichnungsjahr', zustand  INTEGER COMMENT 'Gruppierungsmerkmal', partkey  SMALLINT COMMENT 'the partition key', diff_hash  STRING COMMENT 'Hash for comparing records for inserts.', record_hash  STRING COMMENT 'Hash for all non technical attributes.', insert_tst  TIMESTAMP COMMENT 'Timestamp for creation of record.', source_system  INTEGER COMMENT 'Name of source system.', primary key(bearbidabg, konspartid, lvid, quotausglproperty, varianteid, vtid) disable novalidate ) COMMENT 'Tabelle fuer den quotalen Ausgleich von Kontostaenden' PARTITIONED BY (tenant_name  STRING COMMENT 'Name of the tenant.', process_id  INTEGER COMMENT 'Unique id for ETL-Processes.')  STORED AS parquet;