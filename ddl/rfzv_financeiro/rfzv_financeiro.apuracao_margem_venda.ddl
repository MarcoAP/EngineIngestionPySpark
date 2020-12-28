CREATE DATABASE IF NOT EXISTS rfzv_financeiro;
CREATE VIEW rfzv_financeiro.apuracao_margem_venda 
AS SELECT 
apur.cod_loja cod_loja,
 oper.desc_loja,
 apur.cod_plu cod_plu,
 merc.desc_plu,
 apur.qtd_item_vend qtd_item_vend,
 apur.qtd_item_cancel qtd_item_cancel,
 apur.qtd_item_devol qtd_item_devol,
 apur.qtd_item_total qtd_item_total,
 apur.val_cmv val_cmv,
 apur.val_tot_vend val_tot_venda,
 apur.val_tot_desct val_tot_desct,
 apur.val_tot_cancel val_tot_cancel,
 apur.val_tot_devol val_tot_devol,
 apur.val_vend_bruta_cupom val_vend_bruta_cupom,
 apur.val_vale_comp_fide val_vale_comp_fide,
 apur.val_desct_bonus_cel val_desct_bonus_cel,
 apur.val_vend_bruta_mercad val_vend_bruta_mercad,
 apur.val_icms val_icms,
 apur.val_pis val_pis,
 apur.val_cofins val_cofins,
 apur.val_vend_liq_cupom val_vend_liqcupom,
 apur.val_vend_liq_mercad_gerenc val_vend_liq_mercad_gerenc,
 apur.val_margem_pdv_mercad val_margem_pdv_mercad,
 apur.perc_margem_pdv_mercad perc_margem_pdv_mercad,
 apur.val_preco_med_mercad val_preco_med_mercad,
 apur.datamovto datamovto,
 merc.cod_subgrupo,
 merc.desc_subgrupo,
 merc.cod_grupo,
 merc.desc_grupo,
 merc.cod_subcategoria,
 merc.desc_subcategoria,
 merc.cod_secao,
 merc.desc_secao,
 merc.cod_categoria,
 merc.desc_categoria,
 merc.cod_departamento,
 merc.desc_departamento,
 merc.cod_divisao,
 merc.desc_divisao,
 merc.ind_ativo,
 oper.cod_estrut_geral,
 oper.desc_estrut_geral,
 oper.cod_empr_gru,
 oper.desc_empr_gru,
 oper.cod_empr_sgru,
 oper.desc_empr_sgru,
 oper.cod_bu,
 oper.desc_bu,
 oper.cod_band,
 oper.desc_band,
 oper.cod_regiao,
 oper.desc_regiao,
 oper.cod_region,
 oper.desc_region
 FROM rfzd_financeiro.apuracao_margem_venda apur 
 LEFT OUTER JOIN (SELECT * FROM rfzd_financeiro.estrutura_mercadologica 
 WHERE dt_processamento 
 IN (SELECT max(dt_processamento) 
 FROM rfzd_financeiro.estrutura_mercadologica mercad)) merc 
 ON CAST(apur.cod_plu AS INT) = CAST(merc.cod_plu AS INT) 
 LEFT OUTER JOIN (SELECT * FROM rfzd_financeiro.estrut_oper_fhpgpa 
 WHERE dt_processamento 
 IN (SELECT max(dt_processamento) 
 FROM rfzd_financeiro.estrut_oper_fhpgpa operac)) oper 
 ON CAST(apur.cod_loja AS INT) = CAST(oper.cod_loja AS INT);