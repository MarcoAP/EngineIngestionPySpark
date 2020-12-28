CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.departamento ( 
 cod_depto int COMMENT 'codigo departamento'
,nom_depto string COMMENT 'nome departamento'
,num_seq int COMMENT 'sequencia que o registro aparece no arquivo'
,ind_ativ boolean COMMENT 'indicador ativo'
,dat_proces string COMMENT 'data processamento ingestao'
) STORED AS PARQUET 
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT' = 'Tabela de departamento')
;