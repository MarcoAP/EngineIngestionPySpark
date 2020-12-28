CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.div_sup_div (
 cod_divis int COMMENT 'codigo divisao'
,cod_depto int COMMENT 'codigo departamento'
,dat_ini string COMMENT 'data inicio'
,dat_proces string COMMENT 'data processamento ingestao'
) STORED AS PARQUET 
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT' = 'Tabela de ligacao entre o departamento e a divisao do produto. Carga manual')
;