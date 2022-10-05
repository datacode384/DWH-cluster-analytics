/* VIEWS EXTRACTED FROM V_DDL_CORE_HIST. These views are implemented in 10.85.52.14 core_view database */
/* Hive specifc query*/
/*create view v_pas_lv*/
CREATE OR REPLACE VIEW core_view.v_pas_lv AS
SELECT pas_lv_sid,pas_lv_id,lvid,pdid,generation,mandantid,verzinsbeginn,zweiink,zinsbdep,bearbid,bearbidabg,guebiszins,betrag,waehrungid,lvbegt,lvablt,trdk,btrdiffmig,btrabgl,vstkk,uebverr,lvjahrtag,lvstatus,zb,vzborig,kzvorg,kz_anr,anrewert,annahmeart,fr_switch,let_switch,kz_versst,versst,rentbekz,vertriebsweg,versstnate,errbtgniv,minbtg,zweivorza,btrdiffwsw,kznachz,uebverrnsp,kzanbieterwechsel,kzrelease,antragssteuerung,partkey,kzangvers,kzauszstop,kzabfindungkbr,kollnr,lfdkollnr,verwgrpnr,musterid,zuzahlpol,isvoraussschaedverw,kistamwirkdat,kistambearbdat,kzleistungsarttod,steulandsekundaer,steuzuschlagzuzahl,steuzuschlag,steulandprimaer,kzrechenkern,vorgperformance,c_werbhilf,c_zugweg,c_wikmgl,wirksam_ab,tpa_mandant,gueltig_ab,diff_hash,record_hash,process_id,insert_tst,source_system,
nvl(lead(gueltig_ab-Interval "0.001" second) over(partition by lvid order by bearbid),cast ('2099-12-31 23:59:59.999' as timestamp)) gueltig_bis ,
nvl(lead(wirksam_ab-Interval "0.001" second) over(partition by lvid order by bearbid),cast ('2099-12-31 23:59:59.999' as timestamp)) wirksam_bis ,
nvl(lead(insert_tst-Interval "0.001" second) over(partition by lvid order by insert_tst),cast ('2099-12-31 23:59:59.999' as timestamp))insert_tst_bis
FROM demo_core.pas_lv;

/*create view v_pas_jurlv*/
/*pas_lv_sid column is missing in demo_core.pas_jurlv but view query below has this column in v_ddl_core_hist. 
Perhaps,pas_lv_sid column not imported from join pas_lv with pas_jurlv ? is pas_lv_sid column neeeded in the view of v_pas_jurlv ? */
CREATE OR REPLACE VIEW core_view.v_pas_jurlv AS
SELECT pas_jurlv_sid,pas_jurlv_id,pas_lv_sid,pas_lv_id,lvid,bearbid,zahlwegid,begzahlweg,kzinkassostop,kzmahnstop,kzdynstop,kzbav,kzprovision,gwgident,kzversand,dedat,zusdat,dadat,unverfalldat,kzkapst,lvidext,erhrisiko,kz_getr_veranl,kzbdep,kzkursstell,datantreingbd,datantreingnl,kznateverf,kzrebalance,kzautopol,kzvorsorge,kzsteum,sprachedok,kzpauschbest,kzrentwahl,kzhypzert,kzbearbstop,kzexkassostop,antrdat,angebotnr,ablehngrdid,ablehndat,policedat,erstgrdid,kzabgvt,antrerfassdat,antraenddat,bearbidantrerf,bearbidantraend,antrstatus,dynid,satz,satz2,dyneindat,lastdyn,zykdynamik,mahnstopvondat,mahnstopbisdat,inkstopvondat,inkstopbisdat,abrgrpid,kvidext,exkstopvondat,exkstopbisdat,kzhartz,verkaufsid,inkassostelle,kzvorlversschutz,kzverzichtserkl,empfberprot,mandantid,anbieternrneu,zertifnrneu,vertragsnrneu,kzfinanz,kzversausgl,kzautoabr,angstatus,kzanganonym,angebotidext,benutzerprofil,kzeinwilldatenueb,einwilldatum,landvuid,kzverrenttfl,vorvtbegrent,vorvtrnr,bearbidabg,kzdynzu,kzsteuschaedvtg,kbfantragsnummer,unterschriftdat,mandatid,abwbtgeinzugtag,gesperrt,kzdyneinzubeg,klammerid,kzpfandschutz,vorgangsnruebkap,fristabluebkap,kzsteubehdeda,kzabrregel,kzvorsorgeart,c_kzunisex,c_kzbghurteil,c_kzgeschuetzt,wirksam_ab,tpa_mandant,gueltig_ab,diff_hash,record_hash,process_id,insert_tst,source_system,
nvl(lead(gueltig_ab-Interval "0.001" second) over(partition by lvid order by bearbid),cast ('2099-12-31 23:59:59.999' as timestamp)) gueltig_bis ,
nvl(lead(wirksam_ab-Interval "0.001" second) over(partition by lvid order by bearbid),cast ('2099-12-31 23:59:59.999' as timestamp)) wirksam_bis ,
nvl(lead(insert_tst-Interval "0.001" second) over(partition by lvid order by insert_tst),cast ('2099-12-31 23:59:59.999' as timestamp)) insert_tst_bis
FROM demo_core.pas_jurlv

/*create view v_pas_hvgruppe*/
/*we have duplicate gueltig_ab, wirksam_ab columns in v_ddl_core_hist for lf_ebf_hvgruppe row */
/*pas_lv_sid column is missing in demo_core.pas_jurlv but view query below has this column in v_ddl_core_hist. 
Perhaps,pas_lv_sid column not imported from join pas_lv with pas_hvgruppe ? is pas_lv_sid column neeeded in the view of v_pas_jurlv ? */
CREATE OR REPLACE VIEW core_view.v_pas_hvgruppe AS
SELECT pas_hvgruppe_sid,pas_hvgruppe_id,pas_lv_id,lvid,bearbid,bearbidabg,hvgruppeid,zb,minbtg,uebverr,btrabgl,vstkk,trdk,zweiink,kzvorg,hvtypid,zustand,hauptfaelligkeit,kzteilnovation,maxstoabsch,partkey,kzgrundzulage,pricingid,beg_btgpause,end_btgpause,vzb_vorpause,kzwohnfoerdkto,uebwohnfoerdk,verstke,einwilldatum,vorgabewert,verrenttfl,zbanteilbeginn,zbanteilende,tpid,vorg_btgoptverl,gehzulberech,gehzulbersteig,gehzulberbasjahr,kzberufeinstbonus,kzbtganpassung,crkid,kzzulagebervn,kzzulagebereheg,eigenbtgeheg,eigenbtgbisjahr,eigenbtgehegabw,eigenbtgabwjahr,btgprofil,btgueberz,btgrueckstand,tpvarpiaid,kzzuzgvb,kzfoerdergruppierung,wirksam_ab,tpa_mandant,gueltig_ab,diff_hash,record_hash,process_id,insert_tst,source_system,
nvl(lead(gueltig_ab-Interval "0.001" second) over(partition by lvid order by bearbid),cast ('2099-12-31 23:59:59.999' as timestamp)) gueltig_bis ,
nvl(lead(wirksam_ab-Interval "0.001" second) over(partition by lvid order by bearbid),cast ('2099-12-31 23:59:59.999' as timestamp)) wirksam_bis ,
nvl(lead(insert_tst-Interval "0.001" second) over(partition by lvid order by insert_tst),cast ('2099-12-31 23:59:59.999' as timestamp)) insert_tst_bis
FROM demo_core.pas_hvgruppe

/* Impala specifc query */
CREATE VIEW core_view.v_pas_lv_hist1 AS
SELECT pas_lv_sid,pas_lv_id,lvid,pdid,generation,mandantid,verzinsbeginn,zweiink,zinsbdep,bearbid,bearbidabg,guebiszins,betrag,waehrungid,lvbegt,lvablt,trdk,btrdiffmig,btrabgl,vstkk,uebverr,lvjahrtag,lvstatus,zb,vzborig,kzvorg,kz_anr,anrewert,annahmeart,fr_switch,let_switch,kz_versst,versst,rentbekz,vertriebsweg,versstnate,errbtgniv,minbtg,zweivorza,btrdiffwsw,kznachz,uebverrnsp,kzanbieterwechsel,kzrelease,antragssteuerung,partkey,kzangvers,kzauszstop,kzabfindungkbr,kollnr,lfdkollnr,verwgrpnr,musterid,zuzahlpol,isvoraussschaedverw,kistamwirkdat,kistambearbdat,kzleistungsarttod,steulandsekundaer,steuzuschlagzuzahl,steuzuschlag,steulandprimaer,kzrechenkern,vorgperformance,c_werbhilf,c_zugweg,c_wikmgl,wirksam_ab,tpa_mandant,gueltig_ab,diff_hash,record_hash,process_id,insert_tst,source_system,
nvl(lead(milliseconds_sub(gueltig_ab,1)) over(partition by lvid order by bearbid),cast ('2099-12-31 23:59:59.999' as timestamp)) gueltig_bis ,
nvl(lead(milliseconds_sub(wirksam_ab,1)) over(partition by lvid order by bearbid),cast ('2099-12-31 23:59:59.999' as timestamp)) wirksam_bis ,
nvl(lead(milliseconds_sub(insert_tst,1)) over(partition by lvid order by insert_tst),cast ('2099-12-31 23:59:59.999' as timestamp))insert_tst_bis
FROM demo_core.pas_lv;

/*aktual view for data currently valid */
create or replace view core_view.v_pas_lv_akt as
select s.* from core_view.v_pas_lv_hist s
where (current_timestamp >= s.gueltig_ab and  current_timestamp <= s.gueltig_bis
and current_timestamp >= s.wirksam_ab and  current_timestamp <= s.wirksam_bis
);

/* data currently valid for last ultimo or month end view*/
create or replace view core_view.v_pas_lv_me as;
select r.jahr_mon gueltig_am, s.* from core_view.v_pas_lv_hist s
join demo_core.ref_zeit r
where (r.flag_monat_ende=true
and r.zeit_tst >= s.gueltig_ab and  r.zeit_tst <= s.gueltig_bis
and r.zeit_tst >= s.wirksam_ab and  r.zeit_tst <= s.wirksam_bis
and r.zeit_tst >= cast ('1900-01-01 00:00:00.000' as timestamp)
and r.zeit_tst < cast ('2099-01-01 00:00:00.000' as timestamp)
);

/*select view*/
select lvid, bearbid, gueltig_ab, gueltig_bis,
wirksam_ab, wirksam_bis,
insert_tst, insert_tst_bis
from core_view.v_pas_lv;
/*from core_view.v_pas_jurlv*/
/*from core_view.v_pas_hvgruppe*/


/* -------------------- Views that show Technical metadata about pas / lf tables ---------------------------*/
/*create view TABLE_REPO.v_cognos_tabellen. 
Note: you cannot see the primary key definitions in views although these definitions were migrated from source tables.
For any alternations to views, we have to drop view and recreate them. Alterations like drop, rename etc are possible only on source tables. */
create or replace view TABLE_REPO.v_cognos_tabellen as
select lower(table_id)      table_id,
       lower(source_system) Quellsystem,
       lower(table_name)    Tabellen_name,
       database_view        Quellschema,
       lower(sid_column)    Primary_Key,
       lower(id_column)     Datensatz_id,
       remarks              beschreibung_tabelle
from TABLE_REPO.table
where usage_flag = true;

/*create view TABLE_REPO.v_cognos_tabellen_spalten*/
create or replace view TABLE_REPO.v_cognos_tabellen_spalten as
select lower(table_id)                                    table_id,
       lower(table_name_dwh)                              tabellen_name_dwh,
       lower(column_name)                                 Feld_name,
       dwh_type                                           datentyp,
       dwh_length                                         länge,
       DWH_SCALE                                          scale,
       case when core_sid_col = true then 'X' else '' end Primary_Key_Feld,
       case when core_id_col = true then 'X' else '' end  ID_Feld,
       remarks                                            beschreibung
from TABLE_REPO.v_table_column_all;

/* outer join of above 2 views with create view TABLE_REPO.v_cognos_tabellen_spalten_join
Note: You cannot join 2 tables with duplicate columns except the column which is primary / surroagte unique key in both the views / tables
*/
create or replace view TABLE_REPO.v_cognos_tabellen_spalten_join AS
SELECT t1.beschreibung_tabelle, t1.TABELLEN_NAME, t2.TABELLEN_NAME_DWH, t1.DATENSATZ_ID, t2.DATENTYP, t1.PRIMARY_KEY, t1.QUELLSCHEMA, t1.QUELLSYSTEM,
t2.beschreibung, t2.Feld_name, t2.ID_FELD, t2.LÄNGE, t2.PRIMARY_KEY_FELD, t2."SCALE"
FROM TABLE_REPO.v_cognos_tabellen t1
JOIN
TABLE_REPO.v_cognos_tabellen_spalten t2
ON t2.TABLE_ID = t1.TABLE_ID;

