 SELECT 'LF_' || trim(a.tabschema)||'_'|| a.tabname table_id, trim(colname) src_column_name, a.colno*10+90 src_column_pos,
trim(a.typename) src_type,
a.length/1 as src_length,
a.scale/1 src_scale,
trim(a.nulls) src_not_null,
a.remarks
FROM syscat.columns a
where tabschema = 'EBF' and tabname in ('VB','VP','PARTNERLV','BEGUENST','RISSCHAETZ','STEUSCHICHT','SNTVT','JURVT','PRV','ZAHLEMPF','BEITRAGSVEKTOR')
order by tabname, colno;

create or replace view tabname as (select 'LF_' || trim(tabschema)||'_'|| tabname as table_id,'LF' as source_system, tabschema  table_schema , tabname table_name, tabname || '_ID' as id_column, tabname || '_SID' as sid_column, remarks from syscat.tables
where tabname in ('ABSCHLKO','ANREWERTE','BEARBNWEXT','GARWERTVORGABE','JAHRWERTEINFO','SBBZ','SBINKASSO','SBQUOTAUSGL','SBSTICHBWG','SBUEBVERW','SBUEBZU','SBVERZANS','SKQUOTAUSGL','SKSCHADENRES','SKSGA','SKUEBZU','SKVORAB','SRES','SRESAGGSKBILWERT','SRESEINZEL','STEUAUFTREG','STEUWERTEAUSZAUFT','STEUWERTEUEB','TRANSPARENZGARKO','VBEXT','VBUEBVERW','VERSAUSGL','VERSAUSGLENTN','ZAHLEMPFDAT'));
select * from tabname;