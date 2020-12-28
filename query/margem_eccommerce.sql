select 
ped.cod_loja 
,ped.dat_cria
,ped.cod_plu 
,ped.ind_venda_express 
,sum(ped.qtd_plu_atend) as qtd_plu_vendido 
,sum(ped.val_tot_plu_atend) as val_venda_bruta 
,sum(ped.val_icms) as val_total_icms 
,sum(ped.val_desct) as val_desconto 
,sum(cast(ped.val_taxa_entreg as double)) as val_taxa_entrega  
,sum(ped.val_tot_plu_atend) - sum(ped.val_icms) as val_venda_liquida
from
(
Select cod_loja
,dat_cria
,cod_plu
,ind_venda_express
,qtd_plu_atend
,val_tot_plu_atend
,val_icms
,val_desct
,val_taxa_entreg
from rfzv_financeiro.apur_ped_ecommerce_final 
where cod_plu_simil = 0
and cod_status <> "CA"
and ano_mes_cria >= "2019-04" --2 meses
UNION ALL
Select cod_loja
,dat_cria
,cod_plu_simil as cod_plu
,ind_venda_express
,qtd_plu_atend
,val_tot_plu_atend
,val_icms
,val_desct
,val_taxa_entreg
from rfzv_financeiro.apur_ped_ecommerce_final
where cod_plu_simil <> 0
and cod_status <> "CA"
and ano_mes_cria >= "2019-04" --2 meses
) ped
LEFT JOIN
(SELECT
  t1.cod_loja
 ,t1.cod_plu
 ,CONCAT(SUBSTR(CAST(t1.dat_movto AS STRING),1,4),"-",SUBSTR(CAST(t1.dat_movto AS STRING),5,2),"-",SUBSTR(CAST(t1.dat_movto AS STRING),7,2)) as datamovto
 ,t1.val_unit_custo_estq_final as val_cmv
 FROM rwzd_ge01.td_ge01_dsdpm040_nnvest04 t1
 INNER JOIN (
  SELECT
   cod_loja
  ,cod_plu
  ,dat_movto
  ,MAX(data_referencia) as data_referencia
  FROM rwzd_ge01.td_ge01_dsdpm040_nnvest04
 WHERE CONCAT(SUBSTR(CAST(dat_movto AS STRING),1,4),"-",SUBSTR(CAST(dat_movto AS STRING),5,2),"-",SUBSTR(CAST(dat_movto AS STRING),7,2)) BETWEEN '{0}' and '{1}' --2 meses
  GROUP BY
   cod_loja
  ,cod_plu
  ,dat_movto) t2
 ON  t1.cod_loja = t2.cod_loja
 AND t1.cod_plu = t2.cod_plu
 AND t1.dat_movto = t2.dat_movto
 AND t1.data_referencia = t2.data_referencia
 WHERE CONCAT(SUBSTR(CAST(t1.dat_movto AS STRING),1,4),"-",SUBSTR(CAST(t1.dat_movto AS STRING),5,2),"-",SUBSTR(CAST(t1.dat_movto AS STRING),7,2)) BETWEEN '{0}' and '{1}' --2 meses
 GROUP BY
  t1.cod_loja
 ,t1.cod_plu
 ,CONCAT(SUBSTR(CAST(t1.dat_movto AS STRING),1,4),"-",SUBSTR(CAST(t1.dat_movto AS STRING),5,2),"-",SUBSTR(CAST(t1.dat_movto AS STRING),7,2))
 ,t1.val_unit_custo_estq_final
) custo 
 ON  ped.cod_loja = custo.cod_loja
 AND ped.cod_plu = custo.cod_plu
 AND ped.datamovto = custo.datamovto
group by ped.cod_loja,ped.dat_cria,ped.cod_plu,ped.ind_venda_express
order by ped.cod_loja,ped.cod_plu,ped.dat_cria;



