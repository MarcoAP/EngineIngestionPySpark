--------------------------------------------------------------------------------------------------------------------
--ESTA VIEW DEVERA SER UMA TABELA / FALTA INCLUIR MULTIVAREJO PLU(SIAC COM ind_tipo_venda_exp= LJ)
CREATE VIEW rfzv_financeiro.apur_metr_ecommerce
AS select  
apur.cod_loja
,apur.dat_venda
,apur.cod_plu
,apur.val_tot_vend_bruta_plu
,apur.val_tot_vend_liq_plu
,apur.val_cmv
,apur.val_margem_pdv
,CASE WHEN  ind_tipo_venda_exp = "S"
THEN 'EX' ELSE 'CD' 
END
ind_tipo_venda_exp
, merc.cod_subgrupo
, merc.desc_subgrupo
, merc.cod_grupo
, merc.desc_grupo
, merc.cod_subcategoria
, merc.desc_subcategoria
, merc.cod_secao
, merc.desc_secao
, merc.cod_categoria
, merc.desc_categoria
, merc.cod_departamento
, merc.desc_departamento
, merc.cod_divisao
, merc.desc_divisao
, merc.ind_ativo
, oper.cod_estrut_geral
, oper.desc_estrut_geral
, oper.cod_empr_gru
, oper.desc_empr_gru
, oper.cod_empr_sgru
, oper.desc_empr_sgru
, oper.cod_bu
, oper.desc_bu
, oper.cod_band
, oper.desc_band
, oper.cod_regiao
, oper.desc_regiao
, oper.cod_region
, oper.desc_region
from rfzd_financeiro.apur_metr_ecommerce_plu apur
LEFT OUTER JOIN 
(SELECT * FROM rfzd_financeiro.estrutura_mercadologica 
WHERE dt_processamento IN (SELECT max(dt_processamento) 
FROM rfzd_financeiro.estrutura_mercadologica mercad)) merc 
ON CAST(apur.cod_plu AS INT) = CAST(merc.cod_plu AS INT)
LEFT OUTER JOIN 
(SELECT * FROM rfzd_financeiro.estrut_oper_fhpgpa 
WHERE dt_processamento IN (SELECT max(dt_processamento) 
FROM rfzd_financeiro.estrut_oper_fhpgpa operac)) oper 
ON CAST(apur.cod_loja AS INT) = CAST(oper.cod_loja AS INT)


-----------------------------------------------------------------------------
--QUERY LOJA NAO SERA UTILIZADA 
Select apur_ped.cod_loja
,apur_ped.dat_venda
,apur_ped.venda_bruta 
,apur_ped.venda_liquida 
,apur_ped.val_cmv
,apur_ped.val_margem_pdv
,perfeito.qtd_ped_perf qtd_ped_perf
,completo.qtd_ped_perf qtd_ped_compl
,imperfeito.qtd_ped_perf qtd_ped_incomp
,perfeito.qtd_ped_perf + imperfeito.qtd_ped_perf + completo.qtd_ped_perf qtd_ticket
,perfeito.qtd_ped_perf / (perfeito.qtd_ped_perf + imperfeito.qtd_ped_perf + completo.qtd_ped_perf) perc_ped_perf
,completo.qtd_ped_perf / (perfeito.qtd_ped_perf + imperfeito.qtd_ped_perf + completo.qtd_ped_perf) perc_ped_compl
,imperfeito.qtd_ped_perf / (perfeito.qtd_ped_perf + imperfeito.qtd_ped_perf + completo.qtd_ped_perf) perc_ped_incompl
,apur_ped.venda_bruta / (perfeito.qtd_ped_perf + imperfeito.qtd_ped_perf + completo.qtd_ped_perf) val_ticket_med_ped
,ind_tipo_venda_exp ind_tipo_venda
from(
SELECT 
cod_loja
,dat_venda
,CASE WHEN  ind_tipo_venda_exp = "S"
THEN 'EX' ELSE 'CD' 
END
ind_tipo_venda_exp
,sum(val_tot_vend_bruta_plu) venda_bruta
,sum(val_tot_vend_liq_plu) venda_liquida
,sum(val_cmv) val_cmv
,sum(val_margem_pdv) val_margem_pdv
from rfzd_financeiro.apur_metr_ecommerce_plu
group by cod_loja,dat_venda,ind_tipo_venda_exp) apur_ped
left join 
(SELECT count(1) qtd_ped_perf,cod_loja,dat_venda,classif_compra from(
SELECT cod_loja,dat_venda,num_ped, classif_compra 
from rfzv_financeiro.apur_ped_ecommerce_final
where cod_status <> 'CA'
and 
classif_compra="Compra Perfeita"
GROUP BY cod_loja,dat_venda,num_ped, classif_compra
) pedido
group by cod_loja,dat_venda,classif_compra
) perfeito
ON apur_ped.cod_loja = perfeito.cod_loja
and 
apur_ped.dat_venda = perfeito.dat_venda
left join 
(SELECT count(1) qtd_ped_perf,cod_loja,dat_venda,classif_compra from(
SELECT cod_loja,dat_venda,num_ped, classif_compra 
from rfzv_financeiro.apur_ped_ecommerce_final
where cod_status <> 'CA'
and 
classif_compra="Compra Incompleta"
GROUP BY cod_loja,dat_venda,num_ped, classif_compra
) pedido
group by cod_loja,dat_venda,classif_compra
) imperfeito
ON apur_ped.cod_loja = imperfeito.cod_loja
and 
apur_ped.dat_venda = imperfeito.dat_venda
left join 
(SELECT count(1) qtd_ped_perf,cod_loja,dat_venda,classif_compra from(
SELECT cod_loja,dat_venda,num_ped, classif_compra 
from rfzv_financeiro.apur_ped_ecommerce_final
where cod_status <> 'CA'
and 
classif_compra="Compra Completa"
GROUP BY cod_loja,dat_venda,num_ped, classif_compra
) pedido
group by cod_loja,dat_venda,classif_compra
) completo
ON apur_ped.cod_loja = completo.cod_loja
and 
apur_ped.dat_venda = completo.dat_venda

