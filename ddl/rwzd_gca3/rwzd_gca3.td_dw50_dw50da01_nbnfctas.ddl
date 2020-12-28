CREATE TABLE IF NOT EXISTS rwzd_gca3.td_dw50_dw50da01_nbnfctas (
 dat_mes			STRING COMMENT 'Data composta por ano e mes'
,cod_tipo_bonif		STRING COMMENT 'Codigo do tipo de bonificacao'
,cod_stipo_bonif	STRING COMMENT 'Codigo do subtipo de bonificacao'
,cod_cta			STRING COMMENT 'Codigo da conta'
,cod_orig			STRING COMMENT 'Codigo da origem'
,dt_processamento	STRING COMMENT 'Data de Processamento'
)	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_gca3/td_dw50_dw50da01_nbnfctas/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com: Contas vs Tipo de Bonificacao. Orgiem: GCA3')
;

