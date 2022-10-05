import ibm_db  as db2

def make_connection(database, hostname, port, username, password, timeout):
        return db2.connect('DATABASE={0};'.format(database) +
                  'HOSTNAME={0};'.format(hostname) +
                  'PORT={0};'.format(str(port)) +
                  'PROTOCOL=TCPIP;' +
                  'UID={0};'.format(username) +
                  'PWD={0};'.format(password) +
                  'ConnectTimeout={0};'.format(str(timeout))
                  ,'','')

# connection credentials
def connect_db2():
	return make_connection('db01', '10.85.200.180', '50000', 'db2inst1', '3Hk3fVFQkd3LFCzc', '30')

conn = connect_db2()
print(conn)

drop_table1_if_exists = db2.exec_immediate(conn, "BEGIN IF EXISTS (SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = 'TABLE_REPO' AND TABNAME = 'TABLE_TEST') THEN PREPARE stmt FROM 'DROP TABLE TABLE_REPO.TABLE_TEST';EXECUTE stmt;END IF;END") 

drop_table2_if_exists = db2.exec_immediate(conn, "BEGIN IF EXISTS (SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = 'TABLE_REPO' AND TABNAME = 'TABLE_COLUMN_TEST') THEN PREPARE stmt FROM 'DROP TABLE TABLE_REPO.TABLE_COLUMN_TEST';EXECUTE stmt;END IF;END")

table_creation1=db2.exec_immediate(conn, "CREATE TABLE TABLE_REPO.TABLE_test (TABLE_ID VARCHAR(100 OCTETS) NOT NULL , SOURCE_SYSTEM VARCHAR(100 OCTETS) NOT NULL WITH DEFAULT 'Life Factory' , TABLE_SCHEMA VARCHAR(40 OCTETS) NOT NULL , TABLE_NAME VARCHAR(100 OCTETS) NOT NULL , USAGE_FLAG BOOLEAN NOT NULL WITH DEFAULT false , DATABASE_RAW_VAULT VARCHAR(100 OCTETS) ,DATABASE_CORE VARCHAR(100 OCTETS) , DATABASE_VIEW VARCHAR(100 OCTETS) ,FILE_TYPE_RAW_VAULT VARCHAR(100 OCTETS) NOT NULL WITH DEFAULT 'parquet' , FILE_TYPE_CORE VARCHAR(100 OCTETS) NOT NULL WITH DEFAULT 'parquet' , TABLE_GROUP_ID VARCHAR(100 OCTETS) ,ID_COLUMN VARCHAR(100 OCTETS) ,SID_COLUMN VARCHAR(100 OCTETS) ,NUMBER_OF_BUCKETS INTEGER ,REMARKS VARCHAR(2000 OCTETS) ,REMARKS_EN VARCHAR(2000 OCTETS) ,KEY_TABLE VARCHAR(1000 OCTETS) ,HIST_TYPE VARCHAR(100 OCTETS) )IN TSN_REG_DB01 ORGANIZE BY ROW ;")

table_insertion1 = db2.exec_immediate(conn, "INSERT INTO TABLE_REPO.TABLE_test (TABLE_ID,SOURCE_SYSTEM,TABLE_SCHEMA,TABLE_NAME,USAGE_FLAG,DATABASE_RAW_VAULT,DATABASE_CORE,DATABASE_VIEW,FILE_TYPE_RAW_VAULT,FILE_TYPE_CORE,TABLE_GROUP_ID,ID_COLUMN,SID_COLUMN,NUMBER_OF_BUCKETS,REMARKS,REMARKS_EN,KEY_TABLE,HIST_TYPE) VALUES('LF_EBF_BEARBNW','PAS','EBF     ','BEARBNW',true,'raw_zone','core','core_view','parquet','parquet','LF_EBF','PAS_LV_ID','PAS_BEARBNW_SID',NULL,NULL,NULL,'KEY_PAS_LV',NULL),('LF_EBF_HVGRUPPE','PAS','EBF     ','HVGRUPPE',true,'raw_zone','core','core_view','parquet','parquet','LF_EBF','PAS_HVGRUPPE_ID','PAS_HVGRUPPE_SID',NULL,'Im Rahmen der bAV koennen unterschiedliche Zahlweisen fuer Arbeitgeber (AG) und Arbeitnehmer (AN) vorgegeben werden. Die notwendigen Informationen werden pro HV bzw. in einer hvGruppe gefuehrt, die die eigentliche HV und die zugehoerigen ZV zusammenfas',NULL,'KEY_PAS_HVGRUPPE',NULL),('LF_EBF_JURLV','PAS','EBF     ','JURLV',true,'raw_zone','core','core_view','parquet','parquet','LF_EBF','PAS_JURLV_ID','PAS_JURLV_SID',NULL,'Die Rows in Table jurLV repraesentieren jeweils genau einen Vertrag. Der Vertrag kann aktiv oder abgegangen oder auch ein bearbeiteter oder abgelehnter Antrag sein; massgebend dafuer ist das Field lvStatus desjenigen zugehoerigen, der die hoechste Be',NULL,'KEY_PAS_LV',NULL),('LF_EBF_VT','PAS','EBF     ','VT',true,'raw_zone','core','core_view','parquet','parquet','LF_EBF','PAS_VT_ID','PAS_VT_SID',NULL,'Die Rows in Table vt repraesentieren einen oder mehrere Staende eines; temporal identifiziert durch die Fields lvId und vtId.Fuer welchediese Row gueltig ist, wird durch die Fields bearbId und bearbIdAbg determiniert.Constraint kzrv_ISSET: wenn rvQuote o',NULL,'KEY_PAS_VT',NULL),('LF_EBF_PARTNERLV','PAS','EBF     ','PARTNERLV',true,'raw_zone','core','core_view','parquet','parquet','LF_EBF','PAS_VP_ID','PAS_PARTNERLV_SID',NULL,'Partner werden einzelnen Vertraegen bzw. Vertragsteilen zugeordnet. Dabei wird auch festgehalten, welche Rolle er spielt.Beitragszahler: Der Beitragszahler ist ein Partner zum LV-Vertrag, der die Beitraege bezahlt. Der Beitragszahler kann vom Versicherun',NULL,'KEY_PAS_VP',NULL),('LF_EBF_PRV','PAS','EBF     ','PRV',true,'raw_zone','core','core_view','parquet','parquet','LF_EBF','PAS_LV_ID','PAS_PRV_SID',NULL,'Die fuer einen Vertrag geltende Vereinbarung mit einem VermittlerHistorie-streng: Attribute, die zur strengen Historienfuehrung am Vertrag benoetigt werdenProvisionssatz: Anteil an verschiedenen ProvisionsartenProvisions_Vereinbarung: Die fuer einen Vert',NULL,'KEY_PAS_PRV',NULL),('LF_EBF_RISSCHAETZ','PAS','EBF     ','RISSCHAETZ',true,'raw_zone','core','core_view','parquet','parquet','LF_EBF','PAS_VPVT_ID','PAS_RISSCHAETZ_SID',NULL,'Die Risiko-Einschaetzung ist das Ergebnis der Risikopruefung auf der Grundlage der Risikobeschreibung.Historie-streng: Attribute, die zur strengen Historienfuehrung am Vertrag benoetigt werdenRisikoeinschaetzung: Die Risiko-Einschaetzung ist das Ergebnis',NULL,'KEY_PAS_VPVT','strict'),('LF_EBF_STEUSCHICHT','PAS','EBF     ','STEUSCHICHT',true,'raw_zone','core','core_view','parquet','parquet','LF_EBF','PAS_STEUSCHICHT_ID','PAS_STEUSCHICHT_SID',NULL,'Eine Steuerschicht repraesentiert einen Vertrag in steuerlicher Sicht. Bei teilnovationsrelevanten Aenderung oder novationsrelevanten Aenderungen wird eine neue Steuerschicht angelegt.',NULL,'KEY_PAS_STEUSCHICHT','strict'),('LF_EBF_SNTVT','PAS','EBF     ','SNTVT',true,'raw_zone','core','core_view','parquet','parquet','LF_EBF','PAS_SNTVT_ID','PAS_SNTVT_SID',NULL,'Die Rows in Table sntVt repraesentieren ausgewaehlte Vertragsteileigenschaften nach bestimmten. Die folgenden Non-Key-Columns sind immer besetzt:* tfId* bearbIdRef* schrittIdAndere Columns sind nur nach bestimmtenbesetzt. Der Ausdruck das abgelaufene K',NULL,'KEY_PAS_VT',NULL),('LF_EBF_VB','PAS','EBF     ','VB',true,'raw_zone','core','core_view','parquet','parquet','LF_EBF','PAS_VB_ID','PAS_VB_SID',NULL,'vb',NULL,'KEY_PAS_VB','strict');")

table_creation2=db2.exec_immediate(conn, "CREATE TABLE TABLE_REPO.TABLE_COLUMN_test  (TABLE_ID VARCHAR(100 OCTETS) NOT NULL , SRC_COLUMN_NAME VARCHAR(100 OCTETS) NOT NULL , DWH_COLUMN_NAME VARCHAR(100 OCTETS) , SRC_COLUMN_POS INTEGER NOT NULL WITH DEFAULT 0 , USE_INTERFACE BOOLEAN NOT NULL WITH DEFAULT TRUE , USE_RAW_VAULT BOOLEAN NOT NULL WITH DEFAULT TRUE , USE_CORE BOOLEAN WITH DEFAULT TRUE , SRC_TYPE VARCHAR(100 OCTETS) , SRC_LENGTH INTEGER , SRC_SCALE INTEGER , SRC_NOT_NULL VARCHAR(1 OCTETS) NOT NULL WITH DEFAULT 'N' , CORE_PARTITION_POS INTEGER , CORE_SID_COL BOOLEAN NOT NULL WITH DEFAULT FALSE , CORE_ID_COL BOOLEAN NOT NULL WITH DEFAULT FALSE , CORE_BUCKET_COL INTEGER , CORE_BUCKET_SORT_COL INTEGER , REMARKS VARCHAR(1000 OCTETS) , REMARKS_E VARCHAR(1000 OCTETS) , MESSAGE_FILTER BOOLEAN NOT NULL WITH DEFAULT FALSE )   IN TSN_REG_DB01  ORGANIZE BY ROW ")

table_insertion2=db2.exec_immediate(conn, "INSERT INTO TABLE_REPO.TABLE_COLUMN_test (TABLE_ID,SRC_COLUMN_NAME,DWH_COLUMN_NAME,SRC_COLUMN_POS,USE_INTERFACE,USE_RAW_VAULT,USE_CORE,SRC_TYPE,SRC_LENGTH,SRC_SCALE,SRC_NOT_NULL,CORE_PARTITION_POS,CORE_SID_COL,CORE_ID_COL,CORE_BUCKET_COL,CORE_BUCKET_SORT_COL,REMARKS,REMARKS_E,MESSAGE_FILTER) VALUES ('LF_EBF_BEARBNW','LVID',NULL,100,true,true,true,'VARCHAR',20,0,'N',NULL,true,true,NULL,NULL,NULL,NULL,false),('LF_EBF_BEARBNW','BEARBID',NULL,110,true,true,true,'INTEGER',4,0,'N',NULL,false,false,NULL,NULL,NULL,NULL,false),('LF_EBF_BEARBNW','SCHRITTID',NULL,120,true,true,true,'INTEGER',4,0,'Y',NULL,false,false,NULL,NULL,NULL,NULL,false),('LF_EBF_BEARBNW','BEARBEITERID',NULL,130,true,true,true,'VARCHAR',20,0,'N',NULL,false,false,NULL,NULL,NULL,NULL,false),('LF_EBF_BEARBNW','BEARBDAT',NULL,140,true,true,true,'DATE',4,0,'N',NULL,false,false,NULL,NULL,NULL,NULL,false),('LF_EBF_BEARBNW','WIRKBEGINN',NULL,150,true,true,true,'DATE',4,0,'N',NULL,false,false,NULL,NULL,NULL,NULL,false),('LF_EBF_BEARBNW','FEINBEGINN',NULL,160,true,true,true,'SMALLINT',2,0,'Y',NULL,false,false,NULL,NULL,NULL,NULL,false),('LF_EBF_BEARBNW','WIRKENDE',NULL,170,true,true,true,'DATE',4,0,'N',NULL,false,false,NULL,NULL,NULL,NULL,false),('LF_EBF_BEARBNW','FEINENDE',NULL,180,true,true,true,'SMALLINT',2,0,'Y',NULL,false,false,NULL,NULL,NULL,NULL,false),('LF_EBF_BEARBNW','BEARBIDSTO',NULL,190,true,true,true,'INTEGER',4,0,'N',NULL,false,false,NULL,NULL,NULL,NULL,false);")

db2.close(conn)  
