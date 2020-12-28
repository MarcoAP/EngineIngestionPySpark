CREATE DATABASE IF NOT EXISTS rwzd_ac01_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac01_hist.ext_td_ac01_divisao_superior_divisao (
data_inicio STRING COMMENT 'data de inicio do registro',
cod_depto STRING COMMENT 'codigo do departamento',
cod_divisao STRING COMMENT 'codigo da divisao em que o departamento se encontra'
) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao')
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  "/gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_divisao_superior_divisao"
TBLPROPERTIES ("parquet.compression"="SNAPPY", 'COMMENT' = 'tabela que indica a divisao ao qual o departamento pertence');