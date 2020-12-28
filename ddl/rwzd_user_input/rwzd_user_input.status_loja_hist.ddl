CREATE DATABASE IF NOT EXISTS rwzd_user_input;
CREATE TABLE IF NOT EXISTS rwzd_user_input.status_loja_hist (
 ano_mes     	 STRING COMMENT 'Ano mes'
,cod_loja        STRING COMMENT 'Codigo da loja'
,cod_lfl_rolling STRING COMMENT 'Codigo de status mensal 0 ou 1'
,cod_lfl_fin     STRING COMMENT 'Codigo de status anual 0 ao 5'
)  PARTITIONED BY (   dt_processamento STRING COMMENT 'Data de processamento') 
   STORED AS PARQUET 
   LOCATION '/gpa/rawzone/rwzd_user_input/status_loja_hist' 
   TBLPROPERTIES ('parquet.compression'='SNAPPY','comment'='tabela de status mensal e anual de mesmas lojas')
;