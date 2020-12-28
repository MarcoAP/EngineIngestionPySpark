--------------------------------------------------query Full---------------------------------------------------------
SELECT 
sum(cast(replace(val_receita, ",", ".") as double)) as val_receita_imp
,concat(concat(substr(ano_mes_lancto,1,4), "-", substr(ano_mes_lancto,5,2)), "-", dia_lancto) as dat_lcto
,cod_loja
,conta_ac51
from rwzd_sap.zfii064_gca3_con 
group by dia_lancto,ano_mes_lancto,cod_loja, conta_ac51
------------------------------------------------INCREMENTAL-------------------------------------------------------------
select
zfi.val_receita_imp + coalesce(apur.val_receita_imp,0)  val_receita_imp 
,zfi.dat_lcto dat_lcto ,cast(zfi.cod_loja cod_loja as int)
,zfi.conta_ac51 conta_ac51 
,zfi.dt_processamento dt_processamento
from(
SELECT 
sum(cast(regex_replace(val_receita, ",", ".") as double)) as val_receita_imp
,concat(concat(substr(ano_mes_lancto,1,4), "-", substr(ano_mes_lancto,5,2)), "-", dia_lancto) as dat_lcto
,cod_loja
,conta_ac51 ,dt_processamento
from rwzd_sap.zfii064_gca3_con 
where dt_processamento = '2019-04-24' --d-1 somente os lancamentos do dia anterior
group by dia_lancto,ano_mes_lancto,cod_loja, conta_ac51, dt_processamento) zfi
--order by ano_mes_lancto, dia_lancto, cod_loja, conta_ac51 ;
left join (
select 
val_receita_imp 
,dat_lcto
,cod_loja
,conta_ac51
from rfzd_financeiro.apur_receita_imp) apur ON
cast(zfi.cod_loja as int) = apur.cod_loja and
zfi.conta_ac51  = apur.conta_ac51 and
zfi.dat_lcto = apur.dat_lcto


ano_mes_lancto	201706
cod_loja	1703
dia_lancto	13
conta_ac51	204
val_receita	+0000000000000002,64
dt_processamento	2018-01-17

