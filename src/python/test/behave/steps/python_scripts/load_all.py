"""
This program does all the ETL tasks of ingest, load_rawvault and load_core for every db2 EBF table imported as csv file

usage: spark-submit --properties-file conf/spark_cfg.conf load_all.py

"""

from job_monitoring_functions import *
from pyspark.sql import SparkSession, SQLContext, HiveContext

from abschlko_wrapper_ingest import abschlko_wrapper_ingest
from abschlko_wrapper_rawvault import abschlko_wrapper_rawvault
from abschlko_wrapper_core import abschlko_wrapper_core

from aktstand_wrapper_ingest import aktstand_wrapper_ingest
from aktstand_wrapper_rawvault import aktstand_wrapper_rawvault
from aktstand_wrapper_core import aktstand_wrapper_core

from anrewerte_wrapper_ingest import anrewerte_wrapper_ingest
from anrewerte_wrapper_rawvault import anrewerte_wrapper_rawvault
from anrewerte_wrapper_core import anrewerte_wrapper_core

from antrabw_wrapper_ingest import antrabw_wrapper_ingest
from antrabw_wrapper_rawvault import antrabw_wrapper_rawvault
from antrabw_wrapper_core import antrabw_wrapper_core

from bearbnw_wrapper_ingest import bearbnw_wrapper_ingest
from bearbnw_wrapper_rawvault import bearbnw_wrapper_rawvault
from bearbnw_wrapper_core import bearbnw_wrapper_core

from bearbnwext_wrapper_ingest import bearbnwext_wrapper_ingest
from bearbnwext_wrapper_rawvault import bearbnwext_wrapper_rawvault
from bearbnwext_wrapper_core import bearbnwext_wrapper_core

from beguenst_wrapper_ingest import beguenst_wrapper_ingest
from beguenst_wrapper_rawvault import beguenst_wrapper_rawvault
from beguenst_wrapper_core import beguenst_wrapper_core

from beitragsvektor_wrapper_ingest import beitragsvektor_wrapper_ingest
from beitragsvektor_wrapper_rawvault import beitragsvektor_wrapper_rawvault
from beitragsvektor_wrapper_core import beitragsvektor_wrapper_core

from btgauftreg_wrapper_ingest import btgauftreg_wrapper_ingest
from btgauftreg_wrapper_rawvault import btgauftreg_wrapper_rawvault
from btgauftreg_wrapper_core import btgauftreg_wrapper_core

from drittrecht_wrapper_ingest import drittrecht_wrapper_ingest
from drittrecht_wrapper_rawvault import drittrecht_wrapper_rawvault
from drittrecht_wrapper_core import drittrecht_wrapper_core

from eaklausel_wrapper_ingest import eaklausel_wrapper_ingest
from eaklausel_wrapper_rawvault import eaklausel_wrapper_rawvault
from eaklausel_wrapper_core import eaklausel_wrapper_core

from garwertvorgabe_wrapper_ingest import garwertvorgabe_wrapper_ingest
from garwertvorgabe_wrapper_rawvault import *
from garwertvorgabe_wrapper_core import *

from jurvt_wrapper_ingest import jurvt_wrapper_ingest
from jurvt_wrapper_rawvault import jurvt_wrapper_rawvault
from jurvt_wrapper_core import jurvt_wrapper_core

from hvgruppe_wrapper_ingest import hvgruppe_wrapper_ingest
from hvgruppe_wrapper_rawvault import hvgruppe_wrapper_rawvault
from hvgruppe_wrapper_core import hvgruppe_wrapper_core

from jahreswerte_wrapper_ingest import jahreswerte_wrapper_ingest
from jahreswerte_wrapper_rawvault import jahreswerte_wrapper_rawvault
from jahreswerte_wrapper_core import jahreswerte_wrapper_core

from jahrwerteinfo_wrapper_ingest import *
from jahrwerteinfo_wrapper_rawvault import *
from jahrwerteinfo_wrapper_core import *

from jahrwerteinfofonds_wrapper_ingest import jahrwerteinfofonds_wrapper_ingest
from jahrwerteinfofonds_wrapper_rawvault import jahrwerteinfofonds_wrapper_rawvault
from jahrwerteinfofonds_wrapper_core import jahrwerteinfofonds_wrapper_core

from jurlv_wrapper_ingest import jurlv_wrapper_ingest
from jurlv_wrapper_rawvault import jurlv_wrapper_rawvault
from jurlv_wrapper_core import jurlv_wrapper_core

from kinderzulage_wrapper_ingest import kinderzulage_wrapper_ingest
from kinderzulage_wrapper_rawvault import kinderzulage_wrapper_rawvault
from kinderzulage_wrapper_core import kinderzulage_wrapper_core

from ktofonds_wrapper_ingest import ktofonds_wrapper_ingest
from ktofonds_wrapper_rawvault import ktofonds_wrapper_rawvault
from ktofonds_wrapper_core import ktofonds_wrapper_core

from ktostd_wrapper_ingest import ktostd_wrapper_ingest
from ktostd_wrapper_rawvault import ktostd_wrapper_rawvault
from ktostd_wrapper_core import ktostd_wrapper_core

from lvstand_wrapper_ingest import lvstand_wrapper_ingest
from lvstand_wrapper_rawvault import lvstand_wrapper_rawvault
from lvstand_wrapper_core import lvstand_wrapper_core

from lv_wrapper_ingest import lv_wrapper_ingest
from lv_wrapper_rawvault import lv_wrapper_rawvault
from lv_wrapper_core import lv_wrapper_core

from nbsmappingintext_wrapper_ingest import nbsmappingintext_wrapper_ingest
from nbsmappingintext_wrapper_rawvault import nbsmappingintext_wrapper_rawvault
from nbsmappingintext_wrapper_core import nbsmappingintext_wrapper_core

from notiz_wrapper_ingest import notiz_wrapper_ingest
from notiz_wrapper_rawvault import notiz_wrapper_rawvault
from notiz_wrapper_core import notiz_wrapper_core

from partnerlv_wrapper_ingest import partnerlv_wrapper_ingest                                                                        
from partnerlv_wrapper_rawvault import partnerlv_wrapper_rawvault
from partnerlv_wrapper_core import partnerlv_wrapper_core

from poldarl_wrapper_ingest import poldarl_wrapper_ingest
from poldarl_wrapper_rawvault import poldarl_wrapper_rawvault
from poldarl_wrapper_core import poldarl_wrapper_core

from provdaten_wrapper_ingest import provdaten_wrapper_ingest
from provdaten_wrapper_rawvault import provdaten_wrapper_rawvault
from provdaten_wrapper_core import provdaten_wrapper_core

from prv_wrapper_ingest import prv_wrapper_ingest                                                                                    
from prv_wrapper_rawvault import prv_wrapper_rawvault
from prv_wrapper_core import prv_wrapper_core 

from rismerkmal_wrapper_ingest import rismerkmal_wrapper_ingest
from rismerkmal_wrapper_rawvault import rismerkmal_wrapper_rawvault
from rismerkmal_wrapper_core import rismerkmal_wrapper_core

from risschaetz_wrapper_ingest import risschaetz_wrapper_ingest
from risschaetz_wrapper_rawvault import risschaetz_wrapper_rawvault
from risschaetz_wrapper_core import risschaetz_wrapper_core

from sbbstdbwg_wrapper_ingest import sbbstdbwg_wrapper_ingest
from sbbstdbwg_wrapper_rawvault import sbbstdbwg_wrapper_rawvault
from sbbstdbwg_wrapper_core import sbbstdbwg_wrapper_core

from sbbz_wrapper_ingest import *
from sbbz_wrapper_rawvault import *
from sbbz_wrapper_core import *

from sbinkasso_wrapper_ingest import *
from sbinkasso_wrapper_rawvault import *
from sbinkasso_wrapper_core import *

from sbleistwert_wrapper_ingest import sbleistwert_wrapper_ingest
from sbleistwert_wrapper_rawvault import sbleistwert_wrapper_rawvault
from sbleistwert_wrapper_core import sbleistwert_wrapper_core

from sbquotausgl_wrapper_ingest import *
from sbquotausgl_wrapper_rawvault import *
from sbquotausgl_wrapper_core import *

from sbstichbwg_wrapper_ingest import *
from sbstichbwg_wrapper_rawvault import *
from sbstichbwg_wrapper_core import *

from sbuebverw_wrapper_ingest import *
from sbuebverw_wrapper_rawvault import *
from sbuebverw_wrapper_core import *

from sbuebzu_wrapper_ingest import *
from sbuebzu_wrapper_rawvault import *
from sbuebzu_wrapper_core import *

from sbverzans_wrapper_ingest import *
from sbverzans_wrapper_rawvault import *
from sbverzans_wrapper_core import *

from skbilwert_wrapper_ingest import skbilwert_wrapper_ingest
from skbilwert_wrapper_rawvault import skbilwert_wrapper_rawvault
from skbilwert_wrapper_core import skbilwert_wrapper_core

from skfondsanteil_wrapper_ingest import skfondsanteil_wrapper_ingest
from skfondsanteil_wrapper_rawvault import skfondsanteil_wrapper_rawvault
from skfondsanteil_wrapper_core import skfondsanteil_wrapper_core

from skquotausgl_wrapper_ingest import *
from skquotausgl_wrapper_rawvault import *
from skquotausgl_wrapper_core import *

from skschadenres_wrapper_ingest import *
from skschadenres_wrapper_rawvault import *
from skschadenres_wrapper_core import *

from sksga_wrapper_ingest import *
from sksga_wrapper_rawvault import *
from sksga_wrapper_core import *

from skuebverw_wrapper_ingest import skuebverw_wrapper_ingest
from skuebverw_wrapper_rawvault import skuebverw_wrapper_rawvault
from skuebverw_wrapper_core import skuebverw_wrapper_core

from skuebzu_wrapper_ingest import *
from skuebzu_wrapper_rawvault import *
from skuebzu_wrapper_core import *

from skvorab_wrapper_ingest import *
from skvorab_wrapper_rawvault import *
from skvorab_wrapper_core import *

from sntvt_wrapper_ingest import sntvt_wrapper_ingest
from sntvt_wrapper_rawvault import sntvt_wrapper_rawvault
from sntvt_wrapper_core import sntvt_wrapper_core

from sntvtlstg_wrapper_ingest import sntvtlstg_wrapper_ingest
from sntvtlstg_wrapper_rawvault import sntvtlstg_wrapper_rawvault
from sntvtlstg_wrapper_core import sntvtlstg_wrapper_core

from sresaggskbilwert_wrapper_ingest import *
from sresaggskbilwert_wrapper_rawvault import *
from sresaggskbilwert_wrapper_core import *

from sres_wrapper_ingest import *
from sres_wrapper_rawvault import *
from sres_wrapper_core import *

from sreseinzel_wrapper_ingest import *
from sreseinzel_wrapper_rawvault import *
from sreseinzel_wrapper_core import *

from steuauftreg_wrapper_ingest import *
from steuauftreg_wrapper_rawvault import *
from steuauftreg_wrapper_core import *

from steuschicht_wrapper_ingest import steuschicht_wrapper_ingest
from steuschicht_wrapper_rawvault import steuschicht_wrapper_rawvault
from steuschicht_wrapper_core import steuschicht_wrapper_core

from steuwerte_wrapper_ingest import steuwerte_wrapper_ingest
from steuwerte_wrapper_rawvault import steuwerte_wrapper_rawvault
from steuwerte_wrapper_core import steuwerte_wrapper_core

from steuwerteauszauft_wrapper_ingest import *
from steuwerteauszauft_wrapper_rawvault import *
from steuwerteauszauft_wrapper_core import *

from steuwertekum_wrapper_ingest import steuwertekum_wrapper_ingest
from steuwertekum_wrapper_rawvault import steuwertekum_wrapper_rawvault
from steuwertekum_wrapper_core import steuwertekum_wrapper_core

from steuwertestand_wrapper_ingest import steuwertestand_wrapper_ingest
from steuwertestand_wrapper_rawvault import steuwertestand_wrapper_rawvault
from steuwertestand_wrapper_core import steuwertestand_wrapper_core

from steuwerteueb_wrapper_ingest import *
from steuwerteueb_wrapper_rawvault import *
from steuwerteueb_wrapper_core import *

from transparenz_wrapper_ingest import transparenz_wrapper_ingest
from transparenz_wrapper_rawvault import transparenz_wrapper_rawvault
from transparenz_wrapper_core import transparenz_wrapper_core

from transparenzgarko_wrapper_ingest import *
from transparenzgarko_wrapper_rawvault import *
from transparenzgarko_wrapper_core import *

from vb_wrapper_ingest import vb_wrapper_ingest
from vb_wrapper_rawvault import vb_wrapper_rawvault
from vb_wrapper_core import vb_wrapper_core

from vbext_wrapper_ingest import *
from vbext_wrapper_rawvault import *
from vbext_wrapper_core import *

from vblstg_wrapper_ingest import vblstg_wrapper_ingest
from vblstg_wrapper_rawvault import vblstg_wrapper_rawvault
from vblstg_wrapper_core import vblstg_wrapper_core

from vbuebverw_wrapper_ingest import *
from vbuebverw_wrapper_rawvault import *
from vbuebverw_wrapper_core import *

from versausgl_wrapper_ingest import *
from versausgl_wrapper_rawvault import *
from versausgl_wrapper_core import *

from versausglentn_wrapper_ingest import *
from versausglentn_wrapper_rawvault import *
from versausglentn_wrapper_core import *

from vertschl_wrapper_ingest import vertschl_wrapper_ingest
from vertschl_wrapper_rawvault import vertschl_wrapper_rawvault
from vertschl_wrapper_core import vertschl_wrapper_core

from vp_wrapper_ingest import vp_wrapper_ingest
from vp_wrapper_rawvault import vp_wrapper_rawvault
from vp_wrapper_core import vp_wrapper_core

from vpvt_wrapper_ingest import vpvt_wrapper_ingest
from vpvt_wrapper_rawvault import vpvt_wrapper_rawvault
from vpvt_wrapper_core import vpvt_wrapper_core

from vt_wrapper_ingest import vt_wrapper_ingest
from vt_wrapper_rawvault import vt_wrapper_rawvault
from vt_wrapper_core import vt_wrapper_core

from vtlstg_wrapper_ingest import vtlstg_wrapper_ingest
from vtlstg_wrapper_rawvault import vtlstg_wrapper_rawvault
from vtlstg_wrapper_core import vtlstg_wrapper_core

from vtta_wrapper_ingest import vtta_wrapper_ingest
from vtta_wrapper_rawvault import vtta_wrapper_rawvault
from vtta_wrapper_core import vtta_wrapper_core

from vtpflege_wrapper_ingest import vtpflege_wrapper_ingest
from vtpflege_wrapper_rawvault import vtpflege_wrapper_rawvault
from vtpflege_wrapper_core import vtpflege_wrapper_core

from wertvb_wrapper_ingest import wertvb_wrapper_ingest
from wertvb_wrapper_rawvault import wertvb_wrapper_rawvault
from wertvb_wrapper_core import wertvb_wrapper_core

from zahlempf_wrapper_ingest import zahlempf_wrapper_ingest
from zahlempf_wrapper_rawvault import zahlempf_wrapper_rawvault
from zahlempf_wrapper_core import zahlempf_wrapper_core

from zahlempfdat_wrapper_ingest import *
from zahlempfdat_wrapper_rawvault import *
from zahlempfdat_wrapper_core import *

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

#initialize process_id and start job execution
pid = start_job_execution("ergo","dwh")

#tables = ['ABSCHLKO', 'AKTSTAND', 'ANREWERTE', 'ANTRABW', 'BEARBNW', 'BEARBNWEXT', 'BEGUENST', 'BEITRAGSVEKTOR', 'BTGAUFTREG', 'DRITTRECHT', 'EAKLAUSEL', 'GARWERTVORGABE', 'HVGRUPPE', 'JAHRESWERTE', 'JAHRWERTEINFO', 'JAHRWERTEINFOFONDS', 'JURLV', 'JURVT', 'KINDERZULAGE', 'KTOFONDS', 'KTOSTD', 'LV', 'LVSTAND', 'NBSMAPPINGINTEXT', 'NOTIZ', 'POLDARL', 'PROVDATEN', 'PRV', 'RISMERKMAL', 'RISSCHAETZ', 'SBBSTDBWG', 'SBBZ', 'SBINKASSO', 'SBLEISTWERT', 'SBQUOTAUSGL', 'SBSTICHBWG', 'SBUEBVERW', 'SBUEBZU', 'SBVERZANS', 'SKBILWERT', 'SKFONDSANTEIL', 'SKQUOTAUSGL', 'SKSCHADENRES', 'SKSGA', 'SKUEBVERW', 'SKUEBZU', 'SKVORAB', 'SNTVT', 'SNTVTLSTG', 'SRES', 'SRESAGGSKBILWERT', 'SRESEINZEL', 'STEUAUFTREG', 'STEUSCHICHT', 'STEUWERTE', 'STEUWERTEAUSZAUFT', 'STEUWERTEKUM', 'STEUWERTESTAND', 'STEUWERTEUEB', 'TRANSPARENZ', 'TRANSPARENZGARKO', 'VB', 'VBEXT', 'VBLSTG', 'VBUEBVERW', 'VERSAUSGL', 'VERSAUSGLENTN', 'VERTSCHL', 'VP', 'VPVT', 'VT', 'VTLSTG', 'VTPFLEGE', 'VTTA', 'WERTVB', 'ZAHLEMPF', 'ZAHLEMPFDAT']

tables = ['LVSTAND', 'JURLV', 'RISSCHAETZ', 'VB', 'ZAHLEMPF', 'STEUWERTESTAND', 'VPVT', 'AKTSTAND', 'JAHRESWERTE', 'STEUWERTEKUM', 'LV', 'GARWERTVORGABE', 'SBBZ', 'VERSAUSGLENTN', 'ZAHLEMPFDAT', 'VT']

#call wrappers
if 'ABSCHLKO' in tables:
	abschlko_wrapper_ingest(spark, pid)
	abschlko_wrapper_rawvault(spark, pid)
	abschlko_wrapper_core(spark, pid)
if 'AKTSTAND' in tables:
	aktstand_wrapper_ingest(spark,pid)
	aktstand_wrapper_rawvault(spark,pid)
	aktstand_wrapper_core(spark,pid)
if 'ANREWERTE' in tables:
	anrewerte_wrapper_ingest(spark, pid)
	anrewerte_wrapper_rawvault(spark, pid)
	anrewerte_wrapper_core(spark, pid)
if 'ANTRABW' in tables:
	antrabw_wrapper_ingest(spark,pid)
	antrabw_wrapper_rawvault(spark,pid)
	antrabw_wrapper_core(spark,pid)
if 'BEARBNW' in tables:
	bearbnw_wrapper_ingest(spark, pid)
	bearbnw_wrapper_rawvault(spark,pid)
	bearbnw_wrapper_core(spark,pid)
if 'BEARBNWEXT' in tables:
	bearbnwext_wrapper_ingest(spark, pid)
	bearbnwext_wrapper_rawvault(spark, pid)
	bearbnwext_wrapper_core(spark, pid)
if 'BEGUENST' in tables:
	beguenst_wrapper_ingest(spark, pid)
	beguenst_wrapper_rawvault(spark, pid)
	beguenst_wrapper_core(spark, pid)
if 'BEITRAGSVEKTOR' in tables:
	beitragsvektor_wrapper_ingest(spark, pid)
	beitragsvektor_wrapper_rawvault(spark, pid)
	beitragsvektor_wrapper_core(spark, pid)
if 'BTGAUFTREG' in tables:
	btgauftreg_wrapper_ingest(spark,pid)
	btgauftreg_wrapper_rawvault(spark,pid)
	btgauftreg_wrapper_core(spark,pid)
if 'DRITTRECHT' in tables:
	drittrecht_wrapper_ingest(spark,pid)
	drittrecht_wrapper_rawvault(spark,pid)
	drittrecht_wrapper_core(spark,pid)
if 'EAKLAUSEL' in tables:
	eaklausel_wrapper_ingest(spark,pid)
	eaklausel_wrapper_rawvault(spark,pid)
	eaklausel_wrapper_core(spark,pid)
if 'GARWERTVORGABE' in tables:
	garwertvorgabe_wrapper_ingest(spark, pid)
	garwertvorgabe_wrapper_rawvault(spark, pid)
	garwertvorgabe_wrapper_core(spark, pid)
if 'HVGRUPPE' in tables:
	hvgruppe_wrapper_ingest(spark, pid)
	hvgruppe_wrapper_rawvault(spark, pid)
	hvgruppe_wrapper_core(spark, pid)
if 'JAHRESWERTE' in tables:
	jahreswerte_wrapper_ingest(spark,pid)
	jahreswerte_wrapper_rawvault(spark,pid)
	jahreswerte_wrapper_core(spark,pid)
if 'JAHRWERTEINFO' in tables:
	jahrwerteinfo_wrapper_ingest(spark, pid)
	jahrwerteinfo_wrapper_rawvault(spark, pid)
	jahrwerteinfo_wrapper_core(spark, pid)
if 'JAHRWERTEINFOFONDS' in tables:
	jahrwerteinfofonds_wrapper_ingest(spark,pid)
	jahrwerteinfofonds_wrapper_rawvault(spark,pid)
	jahrwerteinfofonds_wrapper_core(spark,pid)
if 'JURLV' in tables:
	jurlv_wrapper_ingest(spark, pid)
	jurlv_wrapper_rawvault(spark, pid)
	jurlv_wrapper_core(spark, pid)
if 'JURVT' in tables:
	jurvt_wrapper_ingest(spark, pid)
	jurvt_wrapper_rawvault(spark, pid)
	jurvt_wrapper_core(spark, pid)
if 'KINDERZULAGE' in tables:
	kinderzulage_wrapper_ingest(spark, pid)
	kinderzulage_wrapper_rawvault(spark, pid)
	kinderzulage_wrapper_core(spark, pid)
if 'KTOFONDS' in tables:
	ktofonds_wrapper_ingest(spark,pid)
	ktofonds_wrapper_rawvault(spark,pid)
	ktofonds_wrapper_core(spark,pid)
if 'KTOSTD' in tables:
	ktostd_wrapper_ingest(spark,pid)
	ktostd_wrapper_rawvault(spark,pid)
	ktostd_wrapper_core(spark,pid)
if 'LVSTAND' in tables:
	lvstand_wrapper_ingest(spark,pid)
	lvstand_wrapper_rawvault(spark,pid)
	lvstand_wrapper_core(spark,pid)
if 'LV' in tables:
	lv_wrapper_ingest(spark, pid)
	lv_wrapper_rawvault(spark, pid)
	lv_wrapper_core(spark, pid)
if 'NBSMAPPINGINTEXT' in tables:
	nbsmappingintext_wrapper_ingest(spark, pid)
	nbsmappingintext_wrapper_rawvault(spark, pid)
	nbsmappingintext_wrapper_core(spark, pid)
if 'NOTIZ' in tables:
	notiz_wrapper_ingest(spark, pid)
	notiz_wrapper_rawvault(spark, pid)
	notiz_wrapper_core(spark, pid)
if 'PARTNERLV' in tables:
	partnerlv_wrapper_ingest(spark, pid)
	partnerlv_wrapper_rawvault(spark, pid)
	partnerlv_wrapper_core(spark, pid)
if 'POLDARL' in tables:
	poldarl_wrapper_ingest(spark, pid)
	poldarl_wrapper_rawvault(spark, pid)
	poldarl_wrapper_core(spark, pid)
if 'PROVDATEN' in tables:
	provdaten_wrapper_ingest(spark, pid)
	provdaten_wrapper_rawvault(spark, pid)
	provdaten_wrapper_core(spark, pid)
if 'PRV' in tables:
	prv_wrapper_ingest(spark, pid)
	prv_wrapper_rawvault(spark, pid)
	prv_wrapper_core(spark, pid)
if 'RISMERKMAL' in tables:
	rismerkmal_wrapper_ingest(spark, pid)
	rismerkmal_wrapper_rawvault(spark, pid)
	rismerkmal_wrapper_core(spark, pid)
if 'RISSCHAETZ' in tables:
	risschaetz_wrapper_ingest(spark, pid)
	risschaetz_wrapper_rawvault(spark, pid)
	risschaetz_wrapper_core(spark, pid)
if 'SBBSTDBWG' in tables:
	sbbstdbwg_wrapper_ingest(spark, pid)
	sbbstdbwg_wrapper_rawvault(spark, pid)
	sbbstdbwg_wrapper_core(spark, pid)
if 'SBBZ' in tables:
	sbbz_wrapper_ingest(spark, pid)
	sbbz_wrapper_rawvault(spark, pid)
	sbbz_wrapper_core(spark, pid)
if 'SBINKASSO' in tables:
	sbinkasso_wrapper_ingest(spark, pid)
	sbinkasso_wrapper_rawvault(spark, pid)
	sbinkasso_wrapper_core(spark, pid)
if 'SBLEISTWERT' in tables:
	sbleistwert_wrapper_ingest(spark, pid)
	sbleistwert_wrapper_rawvault(spark, pid)
	sbleistwert_wrapper_core(spark, pid)
if 'SBQUOTAUSGL' in tables:
	sbquotausgl_wrapper_ingest(spark, pid)
	sbquotausgl_wrapper_rawvault(spark, pid)
	sbquotausgl_wrapper_core(spark, pid)
if 'SBSTICHBWG' in tables:
	sbstichbwg_wrapper_ingest(spark, pid)
	sbstichbwg_wrapper_rawvault(spark, pid)
	sbstichbwg_wrapper_core(spark, pid)
if 'SBUEBVERW' in tables:
	sbuebverw_wrapper_ingest(spark, pid)
	sbuebverw_wrapper_rawvault(spark, pid)
	sbuebverw_wrapper_core(spark, pid)
if 'SBUEBZU' in tables:
	sbuebzu_wrapper_ingest(spark, pid)
	sbuebzu_wrapper_rawvault(spark, pid)
	sbuebzu_wrapper_core(spark, pid)
if 'SBVERZANS' in tables:
	sbverzans_wrapper_ingest(spark, pid)
	sbverzans_wrapper_rawvault(spark, pid)
	sbverzans_wrapper_core(spark, pid)
if 'SKBILWERT' in tables:
	skbilwert_wrapper_ingest(spark, pid)
	skbilwert_wrapper_rawvault(spark, pid)
	skbilwert_wrapper_core(spark, pid)
if 'SKFONDSANTEIL' in tables:
	skfondsanteil_wrapper_ingest(spark, pid)
	skfondsanteil_wrapper_rawvault(spark, pid)
	skfondsanteil_wrapper_core(spark, pid)
if 'SKQUOTAUSGL' in tables:
	skquotausgl_wrapper_ingest(spark, pid)
	skquotausgl_wrapper_rawvault(spark, pid)
	skquotausgl_wrapper_core(spark, pid)
if 'SKSCHADENRES' in tables:
	skschadenres_wrapper_ingest(spark, pid)
	skschadenres_wrapper_rawvault(spark, pid)
	skschadenres_wrapper_core(spark, pid)
if 'SKSGA' in tables:
	sksga_wrapper_ingest(spark, pid)
	sksga_wrapper_rawvault(spark, pid)
	sksga_wrapper_core(spark, pid)
if 'SKUEBVERW' in tables:
	skuebverw_wrapper_ingest(spark, pid)
	skuebverw_wrapper_rawvault(spark, pid)
	skuebverw_wrapper_core(spark, pid)
if 'SKUEBZU' in tables:
	skuebzu_wrapper_ingest(spark, pid)
	skuebzu_wrapper_rawvault(spark, pid)
	skuebzu_wrapper_core(spark, pid)
if 'SKVORAB' in tables:
	skvorab_wrapper_ingest(spark, pid)
	skvorab_wrapper_rawvault(spark, pid)
	skvorab_wrapper_core(spark, pid)
if 'SNTVT' in tables:
	sntvt_wrapper_ingest(spark, pid)
	sntvt_wrapper_rawvault(spark, pid)
	sntvt_wrapper_core(spark, pid)
if 'SNTVTLSTG' in tables:
	sntvtlstg_wrapper_ingest(spark, pid)
	sntvtlstg_wrapper_rawvault(spark, pid)
	sntvtlstg_wrapper_core(spark, pid)
if 'SRESAGGSKBILWERT' in tables:
	sresaggskbilwert_wrapper_ingest(spark, pid)
	sresaggskbilwert_wrapper_rawvault(spark, pid)
	sresaggskbilwert_wrapper_core(spark, pid)
if 'SRESEINZEL' in tables:
	sreseinzel_wrapper_ingest(spark, pid)
	sreseinzel_wrapper_rawvault(spark, pid)
	sreseinzel_wrapper_core(spark, pid)
if 'SRES' in tables:
	sres_wrapper_ingest(spark, pid)
	sres_wrapper_rawvault(spark, pid)
	sres_wrapper_core(spark, pid)
if 'STEUAUFTREG' in tables:
	steuauftreg_wrapper_ingest(spark, pid)
	steuauftreg_wrapper_rawvault(spark, pid)
	steuauftreg_wrapper_core(spark, pid)
if 'STEUSCHICHT' in tables:
	steuschicht_wrapper_ingest(spark, pid)
	steuschicht_wrapper_rawvault(spark, pid)
	steuschicht_wrapper_core(spark, pid)
if 'STEUWERTEAUSZAUFT' in tables:
	steuwerteauszauft_wrapper_ingest(spark, pid)
	steuwerteauszauft_wrapper_rawvault(spark, pid)
	steuwerteauszauft_wrapper_core(spark, pid)
if 'STEUWERTE' in tables:
	steuwerte_wrapper_ingest(spark, pid)
	steuwerte_wrapper_rawvault(spark, pid)
	steuwerte_wrapper_core(spark, pid)
if 'STEUWERTEKUM' in tables:
	steuwertekum_wrapper_ingest(spark, pid)
	steuwertekum_wrapper_rawvault(spark, pid)
	steuwertekum_wrapper_core(spark, pid)
if 'STEUWERTESTAND' in tables:
	steuwertestand_wrapper_ingest(spark, pid)
	steuwertestand_wrapper_rawvault(spark, pid)
	steuwertestand_wrapper_core(spark, pid)
if 'STEUWERTEUEB' in tables:
	steuwerteueb_wrapper_ingest(spark, pid)
	steuwerteueb_wrapper_rawvault(spark, pid)
	steuwerteueb_wrapper_core(spark, pid)
if 'TRANSPARENZGARKO' in tables:
	transparenzgarko_wrapper_ingest(spark, pid)
	transparenzgarko_wrapper_rawvault(spark, pid)
	transparenzgarko_wrapper_core(spark, pid)
if 'TRANSPARENZ' in tables:
	transparenz_wrapper_ingest(spark, pid)
	transparenz_wrapper_rawvault(spark, pid)
	transparenz_wrapper_core(spark, pid)
if 'VBEXT' in tables:
	vbext_wrapper_ingest(spark, pid)
	vbext_wrapper_rawvault(spark, pid)
	vbext_wrapper_core(spark, pid)
if 'VB' in tables:
	vb_wrapper_ingest(spark, pid)
	vb_wrapper_rawvault(spark, pid)
	vb_wrapper_core(spark, pid)
if 'VBLSTG' in tables:
	vblstg_wrapper_ingest(spark, pid)
	vblstg_wrapper_rawvault(spark, pid)
	vblstg_wrapper_core(spark, pid)
if 'VBUEBVERW' in tables:
	vbuebverw_wrapper_ingest(spark, pid)
	vbuebverw_wrapper_rawvault(spark, pid)
	vbuebverw_wrapper_core(spark, pid)
if 'VERSAUSGLENTN' in tables:
	versausglentn_wrapper_ingest(spark, pid)
	versausglentn_wrapper_rawvault(spark, pid)
	versausglentn_wrapper_core(spark, pid)
if 'VERSAUSGL' in tables:
	versausgl_wrapper_ingest(spark, pid)
	versausgl_wrapper_rawvault(spark, pid)
	versausgl_wrapper_core(spark, pid)
if 'VERTSCHL' in tables:
	vertschl_wrapper_ingest(spark,pid)
	vertschl_wrapper_rawvault(spark,pid)
	vertschl_wrapper_core(spark,pid)
if 'VP' in tables:
	vp_wrapper_ingest(spark, pid)
	vp_wrapper_rawvault(spark, pid)
	vp_wrapper_core(spark, pid)
if 'VPVT' in tables:
	vpvt_wrapper_ingest(spark,pid)
	vpvt_wrapper_rawvault(spark,pid)
	vpvt_wrapper_core(spark,pid)
if 'VT' in tables:
	vt_wrapper_ingest(spark, pid)
	vt_wrapper_rawvault(spark, pid)
	vt_wrapper_core(spark, pid)
if 'VTLSTG' in tables:
	vtlstg_wrapper_ingest(spark, pid)
	vtlstg_wrapper_rawvault(spark, pid)
	vtlstg_wrapper_core(spark, pid)
if 'VTPFLEGE' in tables:
	vtpflege_wrapper_ingest(spark,pid)
	vtpflege_wrapper_rawvault(spark,pid)
	vtpflege_wrapper_core(spark,pid)
if 'VTTA' in tables:
	vtta_wrapper_ingest(spark, pid)
	vtta_wrapper_rawvault(spark,pid)
	vtta_wrapper_core(spark,pid)
if 'WERTVB' in tables:
	wertvb_wrapper_ingest(spark, pid)
	wertvb_wrapper_rawvault(spark, pid)
	wertvb_wrapper_core(spark, pid)
if 'ZAHLEMPFDAT' in tables:
	zahlempfdat_wrapper_ingest(spark, pid)
	zahlempfdat_wrapper_rawvault(spark, pid)
	zahlempfdat_wrapper_core(spark, pid)
if 'ZAHLEMPF' in tables:
	zahlempf_wrapper_ingest(spark, pid)
	zahlempf_wrapper_rawvault(spark, pid)
	zahlempf_wrapper_core(spark, pid)

#set status
set_status(pid, 'finished')

