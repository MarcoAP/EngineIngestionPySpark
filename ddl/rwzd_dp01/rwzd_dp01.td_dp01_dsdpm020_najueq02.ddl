CREATE TABLE IF NOT EXISTS rwzd_dp01.td_dp01_dsdpm020_najueq02 (
 num_seq				STRING COMMENT 'Numero sequencial'
,cod_dep				STRING COMMENT 'Codigo do centro de distribuicao (deposito)'
,cod_reduzido			STRING COMMENT 'Codigo do produto'
,cod_acerto				STRING COMMENT 'Codigo do acerto'
,dat_agend_dep_y2k		STRING COMMENT 'Data de movimento no centro de distribuicao (deposito) para agenda'
,qtd_embal_rec_nf		STRING COMMENT 'Quantidade de produtos recebidos'
,qtd_uni_embal			STRING COMMENT 'Quantidade de unidades por embalagem'
,dt_processamento		STRING COMMENT 'Data de Processamento'
)	PARTITIONED BY (ano_mes_processamento STRING COMMENT 'ano e mes de processamento')
	ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_dp01/td_dp01_dsdpm020_najueq02/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com dados de ajuste de estoque. Orgiem: DP01')
;

