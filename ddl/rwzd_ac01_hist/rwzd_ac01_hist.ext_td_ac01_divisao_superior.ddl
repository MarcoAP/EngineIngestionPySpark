CREATE DATABASE IF NOT EXISTS rwzd_ac01_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac01_hist.ext_td_ac01_divisao_superior (
data_inicio STRING COMMENT 'data de inicio do registro',
cod_divisao STRING COMMENT 'codigo da divisao em que o departamento se encontra',
desc_divisao STRING COMMENT 'descricao da divisao'
) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao')
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  "/gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_divisao_superior"
TBLPROPERTIES ("parquet.compression"="SNAPPY", 'COMMENT' = 'tabela de divisao');