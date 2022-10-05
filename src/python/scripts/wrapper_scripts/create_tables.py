from hdfs_functions import run_cmd

tables =["abschlko","aktstand","anrewerte","antrabw","bearbnw","bearbnwext","beguenst","beitragsvektor","btgauftreg","drittrecht","eaklausel","garwertvorgabe","hvgruppe","jahreswerte","jahrwerteinfo","jahrwerteinfofonds","jurlv","jurvt","kinderzulage","ktofonds","ktostd","lv","lvstand","nbsmappingintext","notiz","partnerlv","poldarl","provdaten","prv","rismerkmal","risschaetz","sbbstdbwg","sbbz","sbinkasso","sbleistwert","sbquotausgl","sbstichbwg","sbuebverw","sbuebzu","sbverzans","skbilwert","skfondsanteil","skquotausgl","skschadenres","sksga","skuebverw","skuebzu","skvorab","sntvt","sntvtlstg","sres","sresaggskbilwert","sreseinzel","steuauftreg","steuschicht","steuwerte","steuwerteauszauft","steuwertekum","steuwertestand","steuwerteueb","transparenz","transparenzgarko","vb","vbext","vblstg","vbuebverw","versausgl","versausglentn","vertschl","vp","vpvt","vt","vtlstg","vtpflege","vtta","wertvb","zahlempf","zahlempfdat"]

tables =["bnka","but000","but020","but0bk","but0id","tiban"]
for table in tables:
	raw = '../../../hive/create_all/raw_zone/par_'+table+'_raw_zone.hql'
	core = '../../../hive/create_all/core/par_'+table+'_core.hql'
	run_cmd(['hive','-f',raw])
	run_cmd(['hive','-f',core])
