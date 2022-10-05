CREATE OR REPLACE VIEW core_view.v_par_bnka_hist AS SELECT diff_hash,gueltig_ab,insert_tst,par_bnka_sid,process_id,record_hash,source_system,z_cre_dida,z_ini_tims,z_logsys_src,z_upd_dida,z_upd_tims,par_bnka_id,mandt,banks,bankl,erdat,ernam,banka,provz,stras,ort01,swift,bgrup,xpgro,loevm,bnklz,tpa_mandant, nvl(lead(gueltig_ab) over(partition by bankl order by insert_tst),cast ('2099-12-31 23:59:59' as timestamp)) gueltig_bis , nvl(lead(insert_tst) over(partition by bankl order by insert_tst),cast ('2099-12-31 23:59:59' as timestamp)) insert_tst_bis  FROM core.par_bnka;