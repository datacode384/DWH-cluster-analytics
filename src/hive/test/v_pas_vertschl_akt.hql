create or replace view test.v_pas_vertschl_akt as select * from core_view.v_pas_vertschl_hist where wirksam_bis = '2099-12-31 23:59:59' and gueltig_bis = '2099-12-31 23:59:59' and insert_tst_bis='2099-12-31 23:59:59';