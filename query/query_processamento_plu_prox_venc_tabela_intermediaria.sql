--Query abaixo Diário

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO rfzd_financeiro.dp_plu_proxv partition(ano_processamento)
Select 
Cast(final.cod_org as int) cod_plu_orig
,Cast(final.cod_alt as int) cod_plu_alt
,final.dt_processamento
,ano_processamento
from(
Select 
dp.cod_plu_orig 
,dp.cod_plu_alt
,tproxv.dt_processamento
,tproxv.cod_plu_orig cod_org
,tproxv.cod_plu_alt cod_alt
,substr(tproxv.dt_processamento,1,4) as ano_processamento
from rwzd_ac01_hist.td_ac01_dsdpm130_tproxv13_tmp tproxv
left join rfzd_financeiro.dp_plu_proxv dp
on Cast(tproxv.cod_plu_orig as int) = dp.cod_plu_orig
and Cast(tproxv.cod_plu_alt as int) = dp.cod_plu_alt
and tproxv.dt_processamento='2019-06-26' --substituir pela data diaria
) final
where final.cod_plu_orig is null
and final.cod_plu_alt is null
group by 
final.cod_org
,final.cod_alt
,final.dt_processamento
,ano_processamento
;

Select * from rfzd_financeiro.dp_plu_proxv


--Query abaixo Histórico
INSERT INTO rfzd_financeiro.dp_plu_proxv partition(ano_processamento)
Select 
Cast(final.cod_org as int) cod_plu_orig
,Cast(final.cod_alt as int) cod_plu_alt
,final.dt_processamento
,ano_processamento
from(
Select 
dp.cod_plu_orig 
,dp.cod_plu_alt
,tproxv.dt_processamento
,tproxv.cod_plu_orig cod_org
,tproxv.cod_plu_alt cod_alt
,substr(tproxv.dt_processamento,1,4) as ano_processamento
from rwzd_ac01_hist.td_ac01_dsdpm130_tproxv13 tproxv
left join rfzd_financeiro.dp_plu_proxv dp
on Cast(tproxv.cod_plu_orig as int) = dp.cod_plu_orig
and Cast(tproxv.cod_plu_alt as int) = dp.cod_plu_alt
) final
where final.cod_plu_orig is null
and final.cod_plu_alt is null
group by 
final.cod_org
,final.cod_alt
,final.dt_processamento
,ano_processamento
;