CREATE DATABASE IF NOT EXISTS rwzd_ac34;
CREATE EXTERNAL TABLE IF NOT EXISTS  rwzd_ac34.ext_td_ac34_ac34rd55_senhas01 (
data_validade	string,
data_envio_email	string,
cod_user	string,
nome_usuario	string,
desc_motivo	string,
cod_ucbd	string,
nome_cplt_ucbd	string,
val_preco_definido_usuario	string,
num_dpto	string,
nome_dpto	string,
num_unng	string,
nome_unng	string,
num_sung	string,
nome_sung	string,
num_seca	string,
nome_seca	string,
idt_plu	string,
desc_cmpl_prod	string,
desc_senha	string,
dt_processamento string
)   ROW FORMAT 
	DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/stg/rwzd_ac34/ext_td_ac34_ac34rd55_senhas01'
	TBLPROPERTIES ('parquet.compression'='SNAPPY')
	;