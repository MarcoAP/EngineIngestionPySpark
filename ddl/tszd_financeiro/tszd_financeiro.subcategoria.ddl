CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.subcategoria ( 
 cod_scat int COMMENT 'codigo subcategoria'
,nom_scat string COMMENT 'nome subcategoria'
,cod_categ int COMMENT 'codigo categoria'
,cod_depto int COMMENT 'codigo departamento'
,dat_proces string COMMENT 'data processamento ingestao'
) STORED AS PARQUET 
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT' = 'Tabela de subcategoria')
;