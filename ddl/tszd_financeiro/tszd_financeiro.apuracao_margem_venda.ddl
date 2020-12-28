CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.apuracao_margem_venda
(
 cod_loja					INT		COMMENT	'Codigo da loja' 
,cod_plu					INT		COMMENT	'Codigo de barra interno do GPA (plu)' 
,qtd_item_vend				DOUBLE	COMMENT	'Quantidade de itens vendidos'
,qtd_item_cancel			DOUBLE	COMMENT	'Quantidade de itens cancelados'
,qtd_item_devol				DOUBLE	COMMENT	'Quantidade de itens devolvidos'
,qtd_item_total				DOUBLE	COMMENT	'Quantidade de itens totais'
,val_cmv					DOUBLE	COMMENT	'Valor do CMV'
,val_tot_vend				DOUBLE	COMMENT	'Valor total vendido'
,val_tot_desct				DOUBLE	COMMENT	'Valor total de descontos'
,val_tot_cancel				DOUBLE	COMMENT	'Valor total de cancelamentos'
,val_tot_devol				DOUBLE	COMMENT	'Valor total de devolucoes'
,val_vend_bruta_cupom		DOUBLE	COMMENT	'Valor da venda bruta cupom'
,val_vale_comp_fide			DOUBLE	COMMENT	'Valor do vale compra fidelidade'
,val_desct_bonus_cel		DOUBLE	COMMENT	'Valor do desconto bonus celular'
,val_vend_bruta_mercad		DOUBLE	COMMENT	'Valor da venda bruta mercadoria'
,val_icms					DOUBLE	COMMENT	'Valor do ICMS'
,val_pis					DOUBLE	COMMENT	'Valor do PIS'
,val_cofins					DOUBLE	COMMENT	'Valor do COFINS'
,val_vend_liq_cupom			DOUBLE	COMMENT	'Valor da venda liquida cupom'
,val_vend_liq_mercad_gerenc	DOUBLE	COMMENT 'Valor da venda liquida mercadoria gerencial'
,val_margem_bruta_gerenc	DOUBLE	COMMENT 'Valor do gross margin gerencial (margem bruta)'
,dt_processamento			STRING COMMENT	'Data do processamento'
)	PARTITIONED BY (datamovto STRING COMMENT 'Data do movimento')
	STORED AS PARQUET
	LOCATION  '/gpa/trustedzone/tszd_financeiro/apuracao_margem_venda/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT' = 'Tabela com apuracao da margem do plu/loja')
;
