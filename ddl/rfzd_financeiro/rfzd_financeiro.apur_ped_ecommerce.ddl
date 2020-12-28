CREATE DATABASE IF NOT EXISTS rfzd_financeiro;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.apur_ped_ecommerce (
cod_loja				int    Comment 'Codigo da Loja'
,dat_venda				String Comment 'Data da Venda'
,num_ped				int    Comment 'Numero do Pedido'
,cod_plu				int    Comment 'Codigo do Produto'
,cod_plu_simil          int    Comment 'Codigo do PLU Similar'
,qtd_plu_ped			double Comment 'Quantidade Produto Pedido'
,qtd_plu_atend			double Comment 'Quantidade Produto Atendido'
,qtd_plu_devol			double Comment 'Quantidade Produto Devolvido'
,ind_status_plu			String Comment 'Indicador Status PLU 1 - Plu Perfeito 2 - Plu Completo 3 - PLU Incompleto'
,classif_compra			String Comment 'Classificacao do Tipo da Compra Compra Perfeita - Compra Completa - Compra Incompleta'
,ind_ruptura  			String Comment 'Indicado de Ruptura de Plu'
,val_unit_ped			double Comment 'Valor Unitario do Pedido'
,val_tot_plu_atend		double Comment 'Valor Total Produto Atendido'
,val_icms				double Comment 'Valor do ICMS'
,val_base_calc_icms		double Comment 'Valor Base Calculo'
,val_desct				double Comment 'Valor do Desconto'
,cod_band				String Comment 'Codigo da Bandeira'
,cod_status				String Comment 'Codigo Status'
,dat_cria				String Comment 'Data da Criacao do Pedido'
,dat_entreg_prev		String Comment 'Data Entrega Prevista'
,dat_entreg_efetua		String Comment 'Data Entrega Efetuada'
,val_taxa_entreg		String Comment 'Valor Taxa de Entrega'
,cod_orig				String Comment 'Codigo Origem'
,cod_tipo_entreg		String Comment 'Codigo Tipo de Entrega'
,cod_ped_site			String Comment 'Codigo Pedido Site' 
,num_cupom_fiscal		String Comment 'Numero do Cupom Fiscal'
,cod_pdv				String Comment 'Codigo do PDV'
,ind_venda_express		String Comment 'Indicador Venda Express'
,dt_processamento		String Comment 'Data de Processamento'
)	PARTITIONED BY (ano_mes_cria STRING Comment 'Ano e Mes da Criacao do Pedido')
	STORED AS PARQUET
	LOCATION '/gpa/refinedzone/rfzd_financeiro/apur_ped_ecommerce/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Tabela Processada com Apuracao de Pedidos do Ecommerce.')
;