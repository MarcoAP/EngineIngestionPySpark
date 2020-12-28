CREATE DATABASE IF NOT EXISTS rwzd_user_input;
CREATE TABLE IF NOT EXISTS rwzd_user_input.metas_ecommerce (
cod_loja STRING COMMENT "Codigo da loja",
cod_tipo_loja STRING COMMENT "Tipo da loja",
dia_meta STRING COMMENT "Dia",
val_meta STRING COMMENT "Valor da meta",
dt_processamento STRING COMMENT "Data de processamento"
) 
STORED AS PARQUET  
LOCATION "/gpa/rawzone/rwzd_user_input/metas_ecommerce"
TBLPROPERTIES ("parquet.compression"="SNAPPY");