CREATE TABLE IF NOT EXISTS rwzd_gca4.td_gca4_gca4dw03_tbnaddp (
 cod_loja					STRING COMMENT 'Codigo da loja'
,cod_plu					STRING COMMENT 'Codigo do produto'
,dat_mov					STRING COMMENT 'Data do movimento'
,cod_tipo_bonif				STRING COMMENT 'Codigo do tipo de bonificacao'
,cod_gru					STRING COMMENT 'Codigo do grupo'
,cod_regiao_comp			STRING COMMENT 'Codigo de regiao da compra'
,cod_fornec					STRING COMMENT 'Codigo do fornecedor'
,num_contr					STRING COMMENT 'Numero do contrato'
,cod_dep					STRING COMMENT 'Codigo do centro de distribuicao(deposito)'
,val_base_custo_sv_base		STRING COMMENT 'Valor base de custo do sobre venda'
,val_aloc					STRING COMMENT 'Valor da alocacao'
,cod_regiao_contr			STRING COMMENT 'Codigo da regiao do contrato'
,ind_tipo_plu				STRING COMMENT 'Indicador do tipo de produto'
,cod_qualif					STRING COMMENT 'Codigo Qualificador de marca propria'
,cod_secao					STRING COMMENT 'Codigo da secao'
)	PARTITIONED BY (dt_processamento STRING COMMENT 'data de processamento')
	ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_gca4/td_gca4_gca4dw03_tbnaddp/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com dados de receita logistica (adicional deposito). Orgiem: GCA4')
;

