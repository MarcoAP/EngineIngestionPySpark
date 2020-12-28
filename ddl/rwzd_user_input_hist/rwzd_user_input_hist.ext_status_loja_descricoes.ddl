CREATE DATABASE IF NOT EXISTS rwzd_user_input_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_user_input_hist.ext_status_loja_descricoes(
	codLoja STRING COMMENT 'Codigo da loja',
	desStatus STRING COMMENT 'Descricao do status'
) PARTITIONED BY (dt_processamento STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  "/gpa/rawzone/stg/rwzd_user_input_hist/ext_status_loja_descricoes"
TBLPROPERTIES ("parquet.compression"="SNAPPY", 'COMMENT' = 'tabela com historico de Descricao de status de loja (ex: Inativo, Expansao, Mesmas Lojas)');