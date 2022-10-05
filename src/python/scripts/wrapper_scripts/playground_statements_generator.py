
tables = ["abschlko","aktstand","anrewerte","antrabw","bearbnw","bearbnwext","beguenst","beitragsvektor","btgauftreg","drittrecht","eaklausel","garwertvorgabe","hvgruppe","jahreswerte","jahrwerteinfo","jahrwerteinfofonds","jurlv","jurvt","kinderzulage","ktofonds","ktostd","lv","lvstand","nbsmappingintext","notiz","partnerlv","poldarl","provdaten","prv","rismerkmal","risschaetz","sbbstdbwg","sbbz","sbinkasso","sbleistwert","sbquotausgl","sbstichbwg","sbuebverw","sbuebzu","sbverzans","skbilwert","skfondsanteil","skquotausgl","skschadenres","sksga","skuebverw","skuebzu","skvorab","sntvt","sntvtlstg","sres","sresaggskbilwert","sreseinzel","steuauftreg","steuschicht","steuwerte","steuwerteauszauft","steuwertekum","steuwertestand","steuwerteueb","transparenz","transparenzgarko","vb","vbext","vblstg","vbuebverw","versausgl","versausglentn","vertschl","vp","vpvt","vt","vtlstg","vtpflege","vtta","wertvb","zahlempf","zahlempfdat"]

target_db = 'demo_core'
source_db = 'core'

with open("playground.sql", "w+") as f:
	for table in tables:
		tn = "pas_"+table
		target_table = target_db + "." + tn
		source_table = source_db + "." + tn
		drop_statement = "drop table if exists "+target_table+" ;\n"
		create_statement = "create table "+target_table+" as select * from "+source_table+" ;\n"
		f.write(drop_statement)
		f.write(create_statement)
