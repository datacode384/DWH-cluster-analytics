create or replace view test.v_pas_steuwertekum_me as select r.zeit_tst gueltig_am, l.* from core_view.v_pas_steuwertekum_hist l cross join demo_core.ref_zeit r where l.gueltig_ab<=r.zeit_tst and l.gueltig_bis>r.zeit_tst and l.insert_tst_bis='2099-12-31 23:59:59' and l.wirksam_ab<=r.zeit_tst and l.wirksam_bis>r.zeit_tst and r.zeit_tst>'1899-12-31 23:59:59' and r.zeit_tst<='2200-12-31 23:59:59' and r.flag_monat_ende=true order by lvid, gueltig_am;