CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.subgrupo ( 
 cod_subgru int COMMENT 'codigo subgrupo'
,nom_subgru string COMMENT 'nome subgrupo'
,cod_status_subgru char(1) COMMENT 'codigo status subgrupo'
,cod_gru int COMMENT 'codigo grupo'
,cod_secao int COMMENT 'codigo secao'
,ind_plangrm char(1) COMMENT 'indicador planogramavel'
,num_seq int COMMENT 'sequencia que o registro aparece no arquivo'
,ind_ativ boolean COMMENT 'indicador ativo'
,dat_proces string COMMENT 'data processamento ingestao'
) STORED AS PARQUET 
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT' = 'Tabela de subgrupo')
;