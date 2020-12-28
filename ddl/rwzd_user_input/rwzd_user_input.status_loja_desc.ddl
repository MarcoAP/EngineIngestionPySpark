CREATE DATABASE IF NOT EXISTS rwzd_user_input;
CREATE TABLE IF NOT EXISTS rwzd_user_input.status_loja_desc (   
 cod_lfl_rolling  STRING  COMMENT 'Status mensal 0 ou 1'
,desc_lfl_rolling STRING  COMMENT 'Descricao do status mensal'
,cod_lfl_fin 	  STRING  COMMENT 'Status anual 0 a 5'
,desc_lfl_fin     STRING  COMMENT 'Descricao do status anual'
)  PARTITIONED BY ( dt_processamento STRING COMMENT 'Data do processamento' ) 
   STORED AS PARQUET 
   LOCATION '/gpa/rawzone/rwzd_user_input/status_loja_descricoes' 
   TBLPROPERTIES ('parquet.compression'='SNAPPY','comment'='tabela de descricao do status da loja')
;