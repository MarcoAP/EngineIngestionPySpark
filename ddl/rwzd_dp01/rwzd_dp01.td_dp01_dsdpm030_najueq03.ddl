CREATE TABLE IF NOT EXISTS rwzd_dp01.td_dp01_dsdpm030_najueq03 (
 num_seq				STRING COMMENT 'Numero sequencial'
,cod_dep				STRING COMMENT 'Codigo do centro de distribuicao (deposito)'
,cod_reduzido			STRING COMMENT 'Codigo do produto'
,ind_sit				STRING COMMENT 'Indicador da situacao'
,dat_ger_form			STRING COMMENT 'Data de geracao do formulario'
,dat_liber				STRING COMMENT 'Data da liberacao'
,qtd_diverg				STRING COMMENT 'Quantidade divergente'
,qtd_diverg_inf			STRING COMMENT 'Quantidade divergente informada'
,qtd_uni_embal			STRING COMMENT 'Quantidade de unidades por embalagem'
,dt_processamento		STRING COMMENT 'Data de Processamento'
)	PARTITIONED BY (ano_processamento STRING COMMENT 'ano de processamento')
	ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_dp01/td_dp01_dsdpm030_najueq03/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com dados de ajuste de estoque. Orgiem: DP01')
;

