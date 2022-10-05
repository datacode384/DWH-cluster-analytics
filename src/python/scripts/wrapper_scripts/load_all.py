from hdfs_functions import *
tables =["abschlko","aktstand","anrewerte","antrabw","bearbnw","bearbnwext","beguenst","beitragsvektor","bom","btgauftreg","drittrecht","eaklausel","enum","enumattr","garwertvorgabe","hvgruppe","jahreswerte","jahrwerteinfo","jahrwerteinfofonds","jurlv","jurvt","kinderzulage","ktofonds","ktostd","lv","lvstand","nbsmappingintext","notiz","partnerlv","pd","pdtf","poldarl","provdaten","prv","pw","risktype","rismerkmal","risschaetz","sbbstdbwg","sbbz","sbinkasso","sbleistwert","sbquotausgl","sbstichbwg","sbuebverw","sbuebzu","sbverzans","skbilwert","skfondsanteil","skquotausgl","skschadenres","sksga","skuebverw","skuebzu","skvorab","sntvt","sntvtlstg","sres","sresaggskbilwert","sreseinzel","steuauftreg","steuschicht","steuwerte","steuwerteauszauft","steuwertekum","steuwertestand","steuwerteueb","tf","transparenz","transparenzgarko","vb","vbext","vblstg","vbuebverw","versausgl","versausglentn","version","vertschl","vp","vpvt","vt","vtlstg","vtpflege","vtta","wertvb","zahlempf","zahlempfdat"]

tables =['bom','enum','enumattr','pd','pdtf','pw','risktype','tf','version']

l, p, error = run_cmd(['python','itergo_jc_start.py'])
print(l,p,error)
if 'bearbnw' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_bearbnw_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_bearbnw_wrapper_core.py'])
	print(l,p,error)
if 'abschlko' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_abschlko_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_abschlko_wrapper_core.py'])
	print(l,p,error)
if 'aktstand' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_aktstand_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_aktstand_wrapper_core.py'])
	print(l,p,error)
if 'anrewerte' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_anrewerte_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_anrewerte_wrapper_core.py'])
	print(l,p,error)
if 'antrabw' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_antrabw_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_antrabw_wrapper_core.py'])
	print(l,p,error)
if 'bearbnwext' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_bearbnwext_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_bearbnwext_wrapper_core.py'])
	print(l,p,error)
if 'beguenst' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_beguenst_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_beguenst_wrapper_core.py'])
	print(l,p,error)
if 'beitragsvektor' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_beitragsvektor_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_beitragsvektor_wrapper_core.py'])
	print(l,p,error)
if 'bom' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_bom_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_bom_wrapper_core.py'])
	print(l,p,error)
if 'btgauftreg' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_btgauftreg_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_btgauftreg_wrapper_core.py'])
	print(l,p,error)
if 'drittrecht' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_drittrecht_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_drittrecht_wrapper_core.py'])
	print(l,p,error)
if 'eaklausel' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_eaklausel_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_eaklausel_wrapper_core.py'])
	print(l,p,error)
if 'enum' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_enum_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_enum_wrapper_core.py'])
	print(l,p,error)
if 'enumattr' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_enumattr_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_enumattr_wrapper_core.py'])
	print(l,p,error)
if 'garwertvorgabe' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_garwertvorgabe_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_garwertvorgabe_wrapper_core.py'])
	print(l,p,error)
if 'hvgruppe' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_hvgruppe_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_hvgruppe_wrapper_core.py'])
	print(l,p,error)
if 'jahreswerte' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_jahreswerte_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_jahreswerte_wrapper_core.py'])
	print(l,p,error)
if 'jahrwerteinfo' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_jahrwerteinfo_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_jahrwerteinfo_wrapper_core.py'])
	print(l,p,error)
if 'jahrwerteinfofonds' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_jahrwerteinfofonds_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_jahrwerteinfofonds_wrapper_core.py'])
	print(l,p,error)
if 'jurlv' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_jurlv_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_jurlv_wrapper_core.py'])
	print(l,p,error)
if 'jurvt' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_jurvt_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_jurvt_wrapper_core.py'])
	print(l,p,error)
if 'kinderzulage' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_kinderzulage_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_kinderzulage_wrapper_core.py'])
	print(l,p,error)
if 'ktofonds' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_ktofonds_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_ktofonds_wrapper_core.py'])
	print(l,p,error)
if 'ktostd' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_ktostd_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_ktostd_wrapper_core.py'])
	print(l,p,error)
if 'lv' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_lv_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_lv_wrapper_core.py'])
	print(l,p,error)
if 'lvstand' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_lvstand_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_lvstand_wrapper_core.py'])
	print(l,p,error)
if 'nbsmappingintext' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_nbsmappingintext_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_nbsmappingintext_wrapper_core.py'])
	print(l,p,error)
if 'notiz' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_notiz_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_notiz_wrapper_core.py'])
	print(l,p,error)
if 'partnerlv' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_partnerlv_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_partnerlv_wrapper_core.py'])
	print(l,p,error)
if 'pd' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_pd_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_pd_wrapper_core.py'])
	print(l,p,error)
if 'pdtf' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_pdtf_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_pdtf_wrapper_core.py'])
	print(l,p,error)
if 'poldarl' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_poldarl_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_poldarl_wrapper_core.py'])
	print(l,p,error)
if 'provdaten' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_provdaten_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_provdaten_wrapper_core.py'])
	print(l,p,error)
if 'prv' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_prv_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_prv_wrapper_core.py'])
	print(l,p,error)
if 'pw' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_pw_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_pw_wrapper_core.py'])
	print(l,p,error)
if 'risktype' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_risktype_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_risktype_wrapper_core.py'])
	print(l,p,error)
if 'rismerkmal' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_rismerkmal_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_rismerkmal_wrapper_core.py'])
	print(l,p,error)
if 'risschaetz' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_risschaetz_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_risschaetz_wrapper_core.py'])
	print(l,p,error)
if 'sbbstdbwg' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sbbstdbwg_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sbbstdbwg_wrapper_core.py'])
	print(l,p,error)
if 'sbbz' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sbbz_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sbbz_wrapper_core.py'])
	print(l,p,error)
if 'sbinkasso' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sbinkasso_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sbinkasso_wrapper_core.py'])
	print(l,p,error)
if 'sbleistwert' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sbleistwert_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sbleistwert_wrapper_core.py'])
	print(l,p,error)
if 'sbquotausgl' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sbquotausgl_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sbquotausgl_wrapper_core.py'])
	print(l,p,error)
if 'sbstichbwg' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sbstichbwg_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sbstichbwg_wrapper_core.py'])
	print(l,p,error)
if 'sbuebverw' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sbuebverw_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sbuebverw_wrapper_core.py'])
	print(l,p,error)
if 'sbuebzu' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sbuebzu_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sbuebzu_wrapper_core.py'])
	print(l,p,error)
if 'sbverzans' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sbverzans_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sbverzans_wrapper_core.py'])
	print(l,p,error)
if 'skbilwert' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_skbilwert_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_skbilwert_wrapper_core.py'])
	print(l,p,error)
if 'skfondsanteil' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_skfondsanteil_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_skfondsanteil_wrapper_core.py'])
	print(l,p,error)
if 'skquotausgl' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_skquotausgl_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_skquotausgl_wrapper_core.py'])
	print(l,p,error)
if 'skschadenres' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_skschadenres_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_skschadenres_wrapper_core.py'])
	print(l,p,error)
if 'sksga' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sksga_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sksga_wrapper_core.py'])
	print(l,p,error)
if 'skuebverw' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_skuebverw_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_skuebverw_wrapper_core.py'])
	print(l,p,error)
if 'skuebzu' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_skuebzu_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_skuebzu_wrapper_core.py'])
	print(l,p,error)
if 'skvorab' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_skvorab_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_skvorab_wrapper_core.py'])
	print(l,p,error)
if 'sntvt' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sntvt_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sntvt_wrapper_core.py'])
	print(l,p,error)
if 'sntvtlstg' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sntvtlstg_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sntvtlstg_wrapper_core.py'])
	print(l,p,error)
if 'sres' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sres_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sres_wrapper_core.py'])
	print(l,p,error)
if 'sresaggskbilwert' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sresaggskbilwert_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sresaggskbilwert_wrapper_core.py'])
	print(l,p,error)
if 'sreseinzel' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_sreseinzel_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_sreseinzel_wrapper_core.py'])
	print(l,p,error)
if 'steuauftreg' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_steuauftreg_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_steuauftreg_wrapper_core.py'])
	print(l,p,error)
if 'steuschicht' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_steuschicht_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_steuschicht_wrapper_core.py'])
	print(l,p,error)
if 'steuwerte' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_steuwerte_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_steuwerte_wrapper_core.py'])
	print(l,p,error)
if 'steuwerteauszauft' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_steuwerteauszauft_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_steuwerteauszauft_wrapper_core.py'])
	print(l,p,error)
if 'steuwertekum' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_steuwertekum_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_steuwertekum_wrapper_core.py'])
	print(l,p,error)
if 'steuwertestand' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_steuwertestand_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_steuwertestand_wrapper_core.py'])
	print(l,p,error)
if 'steuwerteueb' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_steuwerteueb_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_steuwerteueb_wrapper_core.py'])
	print(l,p,error)
if 'tf' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_tf_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_tf_wrapper_core.py'])
	print(l,p,error)
if 'transparenz' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_transparenz_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_transparenz_wrapper_core.py'])
	print(l,p,error)
if 'transparenzgarko' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_transparenzgarko_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_transparenzgarko_wrapper_core.py'])
	print(l,p,error)
if 'vb' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vb_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vb_wrapper_core.py'])
	print(l,p,error)
if 'vbext' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vbext_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vbext_wrapper_core.py'])
	print(l,p,error)
if 'vblstg' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vblstg_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vblstg_wrapper_core.py'])
	print(l,p,error)
if 'vbuebverw' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vbuebverw_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vbuebverw_wrapper_core.py'])
	print(l,p,error)
if 'versausgl' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_versausgl_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_versausgl_wrapper_core.py'])
	print(l,p,error)
if 'versausglentn' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_versausglentn_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_versausglentn_wrapper_core.py'])
	print(l,p,error)
if 'version' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_version_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_version_wrapper_core.py'])
	print(l,p,error)
if 'vertschl' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vertschl_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vertschl_wrapper_core.py'])
	print(l,p,error)
if 'vp' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vp_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vp_wrapper_core.py'])
	print(l,p,error)
if 'vpvt' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vpvt_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vpvt_wrapper_core.py'])
	print(l,p,error)
if 'vt' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vt_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vt_wrapper_core.py'])
	print(l,p,error)
if 'vtlstg' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vtlstg_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vtlstg_wrapper_core.py'])
	print(l,p,error)
if 'vtpflege' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vtpflege_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vtpflege_wrapper_core.py'])
	print(l,p,error)
if 'vtta' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_vtta_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_vtta_wrapper_core.py'])
	print(l,p,error)
if 'wertvb' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_wertvb_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_wertvb_wrapper_core.py'])
	print(l,p,error)
if 'zahlempf' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_zahlempf_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_zahlempf_wrapper_core.py'])
	print(l,p,error)
if 'zahlempfdat' in tables:
	l,p,error = run_cmd(['spark-submit','--jars','db2jcc4.jar','raw_zone/pas_zahlempfdat_wrapper_rawvault.py'])
	print(l,p,error)
	l,p,error = run_cmd(['spark-submit','core/pas_zahlempfdat_wrapper_core.py'])
	print(l,p,error)
l,p,error = run_cmd(['python','itergo_jc_stop.py'])
