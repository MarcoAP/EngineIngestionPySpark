CREATE DATABASE IF NOT EXISTS rfzd_financeiro;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.apur_metr_ecommerce_plu (
cod_loja					int    Comment 'Codigo da Loja'
,dat_venda					String Comment 'Data Da venda'
,cod_plu					int    Comment 'Codigo Produto'
,qtd_plu_vend				double Comment 'Quantidade plu vendido'
,val_tot_vend 				double Comment 'Valor Total Vendido'
,val_tot_icms				double Comment 'Valor Total ICMS'
,val_tot_desct				double Comment 'Valor Total Desconto'
,val_tot_vend_bruta_plu		double Comment 'Valor Total Venda Bruta PLU'
,val_tot_vend_liq_plu		double Comment 'Valor Total Venda Liquida PLU'
,val_cmv					double Comment 'Valor CMV - Custo da Mercadoria Vendida'
,val_margem_pdv				double Comment 'Valor Margem PDV'
,ind_tipo_vend_exp			String Comment 'Indicador Tipo Venda EXPRESS S = SIM / N = Nao'
,dt_processamento			String Comment 'Data de Processamento'
)	PARTITIONED BY (mes_processamento STRING Comment 'Mes de Processamento')
	STORED AS PARQUET
	LOCATION '/gpa/refinedzone/rfzd_financeiro/apur_metr_ecommerce_plu/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Tabela Processada com Apuracao de Metricas do Ecommerce por plu.')
;
