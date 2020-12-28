CREATE DATABASE IF NOT EXISTS rwzd_user_input_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_user_input_hist.ext_status_loja_hist (

	anoMes STRING		COMMENT 'Ano e mes do status de mesmas lojas formato yyyymm',
	codLoja STRING  	COMMENT 'Codigo da loja',
	codStatus STRING 	COMMENT 'Codigo de status de mesmas lojas'
	
) PARTITIONED BY (dt_processamento STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  "/gpa/rawzone/stg/rwzd_user_input_hist/ext_status_loja_hist"
TBLPROPERTIES ("parquet.compression"="SNAPPY",'COMMENT' = 'tabela com historico da condicao de status mensal das lojas');