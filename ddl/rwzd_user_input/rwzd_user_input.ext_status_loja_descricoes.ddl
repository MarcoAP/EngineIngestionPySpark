CREATE DATABASE IF NOT EXISTS rwzd_user_input;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_user_input.ext_status_loja_descricoes(
	cod_status_class_loja STRING COMMENT 'Codigo do status de classificacao da loja(Mesmas Lojas, Inativo,Regularizacao, etc)',
	desc_status_class_loja STRING COMMENT 'Descricao do status de classificacao da loja (Mesmas Lojas, Inativo,Regularizacao, etc)'
) PARTITIONED BY (DT_PROCESSAMENTO STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;' LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  '/gpa/rawzone/stg/rwzd_user_input/ext_status_loja_descricoes'
TBLPROPERTIES ("parquet.compression"="SNAPPY", 'COMMENT' = 'tabela de Descricao de status de loja (ex: Inativo, Expansao, Mesmas Lojas)');