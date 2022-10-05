CREATE OR REPLACE VIEW core_view.v_par_but0bk_hist AS SELECT diff_hash,gueltig_ab,insert_tst,par_but0bk_sid,process_id,record_hash,source_system,wirksam_ab,z_cre_dida,z_ini_tims,z_logsys_src,z_upd_dida,z_upd_tims,par_but0bk_id,mandt,partner,bkvid,banks,bankl,bankn,bkont,bkref,koinh,bkext,xezer,accname,move_bkvid,tpa_mandant, nvl(lead(gueltig_ab) over(partition by partner order by insert_tst),cast ('2099-12-31 23:59:59' as timestamp)) gueltig_bis , nvl(lead(insert_tst) over(partition by partner order by insert_tst),cast ('2099-12-31 23:59:59' as timestamp)) wirksam_bis , nvl(lead(insert_tst) over(partition by partner order by insert_tst),cast ('2099-12-31 23:59:59' as timestamp)) insert_tst_bis  FROM core.par_but0bk;