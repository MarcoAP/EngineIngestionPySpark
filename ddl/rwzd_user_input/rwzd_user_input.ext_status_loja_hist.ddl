CREATE DATABASE IF NOT EXISTS rwzd_user_input;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_user_input.ext_status_loja_hist (
	ano_mes STRING		COMMENT 'Ano e mes do status de mesmas lojas formato yyyymm',
	cod_loja STRING  	COMMENT 'Codigo da loja',
	cod_status_class_loja_fixo STRING COMMENT 'Codigo do status de classificacao da loja na visao fixa(Mesmas Lojas, Inativo,Regularizacao, etc)',
	cod_status_class_loja_movel STRING COMMENT 'Codigo do status de classificacao da loja na visao movel(Mesmas Lojas, Inativo,Regularizacao, etc)'
) PARTITIONED BY (DT_PROCESSAMENTO STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;' LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  '/gpa/rawzone/stg/rwzd_user_input/ext_status_loja_hist'
TBLPROPERTIES ("parquet.compression"="SNAPPY",'COMMENT' = 'tabela da condicao de status mensal das lojas');