CREATE DATABASE IF NOT EXISTS rwzd_amelia;
CREATE TABLE IF NOT EXISTS rwzd_amelia.plu_pedido (
 num_ped				String Comment 'Numero do Pedido'
,cod_plu				String Comment 'Codigo do Produto'
,qtd_plu_ped			String Comment 'Quantidade Produto Pedido'
,qtd_plu_atend			String Comment 'Quantidade Produto Atendido'
,qtd_plu_devol			String Comment 'Quantidade Produto Devolvido'
,val_unit_ped			String Comment 'Valor Unitario do Pedido'
,val_tot_plu_atend		String Comment 'Valor Total Produto Atendido'
,ind_aceit_plu_simil	String Comment 'Indicador Aceitacao Produto Similar'
,cod_plu_simil			String Comment 'Codigo Produto Similar'
,val_icms				String Comment 'Valor do ICMS'
,val_base_calc_icms		String Comment 'Valor Base Calculo'
,cod_loja				String Comment 'Codigo da Loja'
,val_desct				String Comment 'Valor do Desconto'
,ind_presen				String Comment 'Indicador Presente'
,dt_processamento		String Comment 'Data de processamento'
)	PARTITIONED BY (ano_mes_cria STRING Comment 'Ano e Mes da Criacao do Pedido')
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_amelia/plu_pedido/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com relacao de plu e pedido. Orgiem: Amelia')
;
