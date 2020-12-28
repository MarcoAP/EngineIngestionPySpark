CREATE TABLE IF NOT EXISTS rwzd_gca4.td_gca4_gca4dw02_ntalc (
 cod_loja			STRING COMMENT 'Codigo da loja'
,cod_secao			STRING COMMENT 'Codigo da secao'
,cod_gru			STRING COMMENT 'Codigo do grupo'
,cod_fornec			STRING COMMENT 'Codigo do fornecedor'
,cod_plu			STRING COMMENT 'Codigo do produto'
,cod_tipo_bonif		STRING COMMENT 'Codigo do tipo de bonificacao'
,cod_stipo_bonif	STRING COMMENT 'Codigo do subtipo de bonificacao'
,dat_bonif			STRING COMMENT 'Data da bonificacao'
,val_bonif			STRING COMMENT 'Valor da bonificacao'
,dt_processamento   STRING COMMENT 'Data de Processamento'
)	PARTITIONED BY (ano_processamento STRING COMMENT 'ano de processamento')
	ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_gca4/td_gca4_gca4dw02_ntalc/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com dados de alocacoes. Orgiem: GCA4')
;

