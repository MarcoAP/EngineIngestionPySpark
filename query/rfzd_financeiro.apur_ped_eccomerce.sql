Create table wrkd_financeiro.tmp_table_apur_comp_ecommerce
as
select 
Cast(final.cod_loja as int) cod_loja,
final.dat_venda,
Cast(final.num_ped as int) num_ped,
Cast(final.cod_plu as int) cod_plu,
Cast(final.cod_plu_simil as int) cod_plu_simil,
Cast(final.qtd_plu_ped as double) qtd_plu_ped,
Cast(final.qtd_plu_atend as double) qtd_plu_atend,
Cast(final.qtd_plu_devol as double) qtd_plu_devol,
final.ind_status_plu,
final.ind_ruptura,
Cast(final.val_unit_ped as double) val_unit_ped,
Cast(final.val_tot_plu_atend as double) val_tot_plu_atend,
Cast(final.val_icms as double) val_icms,
Cast(final.val_base_calc_icms as double) val_base_calc_icms,
Cast(final.val_desct as double) val_desct,
final.cod_band,
final.cod_status,
final.dat_cria,
final.dat_entreg_prev,
final.dat_entreg_efetua,
final.val_taxa_entreg,
final.cod_orig,
final.cod_tipo_entreg,
final.cod_ped_site,
final.num_cupom_fiscal,
final.cod_pdv,
final.ind_venda_express,
final.dt_processamento,
(max(ind_status_plu) over (partition by final.num_ped)) as classif_compra
from (
SELECT 
ped.cod_loja, 
ped.dat_venda,
plu.cod_plu,
plu.cod_plu_simil,
plu.qtd_plu_ped,
plu.qtd_plu_atend,
plu.qtd_plu_devol,
plu.val_unit_ped,
plu.val_tot_plu_atend,
plu.val_icms,
plu.val_base_calc_icms,
plu.val_desct,
ped.cod_band,
ped.cod_status,
ped.dat_cria,
ped.dat_entreg_prev,
ped.dat_entreg_efetua,
ped.val_taxa_entreg,
ped.cod_orig,
ped.cod_tipo_entreg,
ped.cod_ped_site,
ped.num_cupom_fiscal,
plu.num_ped,
plu.dt_processamento,
ped.cod_pdv,
ped.ind_venda_express,
Case 
When (Cast(plu.qtd_plu_atend as int) >= (Cast(plu.qtd_plu_ped as int)) and cast(plu.cod_plu_simil as int) = 0 ) then 1
When (Cast(plu.qtd_plu_atend as int) >= (Cast(plu.qtd_plu_ped as int)) and cast(plu.cod_plu_simil as int) > 0 ) then 2
When (Cast(plu.qtd_plu_atend as int) <  (Cast(plu.qtd_plu_ped as int)) and cast(plu.cod_plu_simil as int) >= 0 ) then 3
END as ind_status_plu,
Case
When ((Cast(plu.qtd_plu_atend as int)) < (Cast(plu.qtd_plu_ped as int))) then 'S' else 'N'
END as ind_ruptura
FROM rwzd_amelia.pedido ped
INNER JOIN rwzd_amelia.plu_pedido plu ON
ped.num_ped = plu.num_ped
where ped.cod_status in ('EX','EP','AP','CA')
and ped.dt_processamento = '2019-04-16'
GROUP BY 
ped.cod_loja,
ped.dat_venda,
plu.num_ped,
plu.cod_plu,
plu.cod_plu_simil,
plu.qtd_plu_ped,
plu.qtd_plu_atend,
plu.qtd_plu_devol,
plu.val_unit_ped,
ind_status_plu,
plu.val_tot_plu_atend,
plu.val_icms,
plu.val_base_calc_icms,
plu.val_desct,
ped.cod_band,
ped.cod_status,
ped.dat_cria,
ped.dat_entreg_prev,
ped.dat_entreg_efetua,
ped.val_taxa_entreg,
ped.cod_orig,
ped.cod_tipo_entreg,
ped.cod_ped_site,
ped.num_cupom_fiscal,
ped.cod_pdv,
ped.ind_venda_express,
plu.dt_processamento
) final
where dt_processamento = '2019-04-16'