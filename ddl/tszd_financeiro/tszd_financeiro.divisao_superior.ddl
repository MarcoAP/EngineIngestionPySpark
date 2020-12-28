CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.divisao_superior (
 cod_divis int COMMENT 'codigo divisao'
,nom_divis string COMMENT 'nome da divisao'
,dat_ini string COMMENT 'data inicio'
,dat_proces string COMMENT 'data processamento ingestao'
) STORED AS PARQUET 
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT' = 'Tabela de divisao. Carga manual')
;