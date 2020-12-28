CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.categoria ( 
 cod_categ int COMMENT 'codigo categoria'
,nom_categ string COMMENT 'nome categoria'
,cod_depto int COMMENT 'codigo departamento'
,dat_proces string COMMENT 'data processamento ingestao'
) STORED AS PARQUET 
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT' = 'Tabela de categoria')
;