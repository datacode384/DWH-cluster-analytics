create or replace view test.v_par_tiban_akt as select * from core_view.v_par_tiban_hist where insert_tst_bis='2099-12-31 23:59:59' and gueltig_bis='2099-12-31 23:59:59' and wirksam_bis='2099-12-31 23:59:59';