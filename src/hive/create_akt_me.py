from hdfs_functions import *
import os
tables =["abschlko","aktstand","anrewerte","antrabw","bearbnw","bearbnwext","beguenst","beitragsvektor","btgauftreg","drittrecht","eaklausel","garwertvorgabe","hvgruppe","jahreswerte","jahrwerteinfo","jahrwerteinfofonds","jurlv","jurvt","kinderzulage","ktofonds","ktostd","lv","lvstand","nbsmappingintext","notiz","partnerlv","poldarl","provdaten","prv","rismerkmal","risschaetz","sbbstdbwg","sbbz","sbinkasso","sbleistwert","sbquotausgl","sbstichbwg","sbuebverw","sbuebzu","sbverzans","skbilwert","skfondsanteil","skquotausgl","skschadenres","sksga","skuebverw","skuebzu","skvorab","sntvt","sntvtlstg","sres","sresaggskbilwert","sreseinzel","steuauftreg","steuschicht","steuwerte","steuwerteauszauft","steuwertekum","steuwertestand","steuwerteueb","transparenz","transparenzgarko","vb","vbext","vblstg","vbuebverw","versausgl","versausglentn","vertschl","vp","vpvt","vt","vtlstg","vtpflege","vtta","wertvb","zahlempf","zahlempfdat"]
tables = ["but020", "tiban", "but0id", "but0bk", "but000", "bnka"]
tables = ['vt']
for table in tables:
	me = "create or replace view test.v_pas_"+table+"_me as select r.zeit_tst gueltig_ab, l.* from core_view.v_pas_"+table+"_hist l cross join demo_core.ref_zeit r where l.gueltig_ab<=r.zeit_tst and l.gueltig_bis>r.zeit_tst and l.insert_tst_bis='2099-12-31 23:59:59' and l.wirksam_ab<=r.zeit_tst and l.wirksam_bis>r.zeit_tst and r.zeit_tst>'1899-12-31 23:59:59' and r.zeit_tst<='2200-12-31 23:59:59' and r.flag_monat_ende=true order by lvid, gueltig_am;"
	akt = "create or replace view test.v_pas_"+table+"_akt as select * from core_view.v_pas_"+table+"_hist where wirksam_bis = '2099-12-31 23:59:59' and gueltig_bis = '2099-12-31 23:59:59' and insert_tst_bis='2099-12-31 23:59:59';"
	"""
	if table not in ['bnka']:
		me = "create or replace view test.v_par_"+table+"_me as select r.zeit_tst gueltig_am, l.* from core_view.v_par_"+table+"_hist l cross join demo_core.ref_zeit r where insert_tst<=r.zeit_tst<insert_tst_bis and gueltig_ab<=r.zeit_tst<gueltig_bis and wirksam_ab<=r.zeit_tst<wirksam_bis and flag_monat_ende = true order by partner, gueltig_am;"
		akt = "create or replace view test.v_par_"+table+"_akt as select * from core_view.v_par_"+table+"_hist where insert_tst_bis='2099-12-31 23:59:59' and gueltig_bis='2099-12-31 23:59:59' and wirksam_bis='2099-12-31 23:59:59';"
	else:
		me = "create or replace view test.v_par_"+table+"_me as select r.zeit_tst gueltig_am, l.* from core_view.v_par_"+table+"_hist l cross join demo_core.ref_zeit r where insert_tst<=r.zeit_tst<insert_tst_bis and gueltig_ab<=r.zeit_tst<gueltig_bis and flag_monat_ende = true order by bankl, gueltig_am;"
		akt = "create or replace view test.v_par_"+table+"_akt as select * from core_view.v_par_"+table+"_hist where insert_tst_bis='2099-12-31 23:59:59' and gueltig_bis='2099-12-31 23:59:59';"
	if table in ['tiban']:
		me = "create or replace view test.v_par_"+table+"_me as select r.zeit_tst gueltig_am, l.* from core_view.v_par_"+table+"_hist l cross join demo_core.ref_zeit r where insert_tst<=r.zeit_tst<insert_tst_bis and gueltig_ab<=r.zeit_tst<gueltig_bis and wirksam_ab<=r.zeit_tst<wirksam_bis and flag_monat_ende = true order by bankl, gueltig_am;"
	akt_file = "v_par_"+table+"_akt.hql"
	me_file = "v_par_"+table+"_me.hql"
	"""
	akt_file = "v_pas_"+table+"_akt.hql"
	me_file = "v_pas_"+table+"_me.hql"
	with open(os.getcwd() + "/test/"+ akt_file, "w+") as f:
		f.write(akt)
	with open(os.getcwd() + "/test/"+ me_file, "w+") as f:
		f.write(me)
	l, p, error = run_cmd(["hive","-f","test/"+akt_file])
	print(l,p,error)
	#l, p, error= run_cmd(["hive","-f","test/"+me_file])
	#print(l,p,error)
