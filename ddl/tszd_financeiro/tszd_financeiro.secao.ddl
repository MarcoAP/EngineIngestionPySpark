CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.secao (
 cod_secao int COMMENT 'codigo secao'
,nom_secao string COMMENT 'nome secao'
,cod_depto int COMMENT 'codigo departamento'
,num_seq int COMMENT 'sequencia que o registro aparece no arquivo'
,ind_ativ boolean COMMENT 'indicador ativo'
,dat_proces string COMMENT 'data processamento ingestao'
) STORED AS PARQUET 
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT' = 'Tabela de secao')
;