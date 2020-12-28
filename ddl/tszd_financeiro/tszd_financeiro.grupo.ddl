CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.grupo ( 
 cod_gru int COMMENT 'codigo grupo'
,nom_gru string COMMENT 'nome grupo'
,cod_status_gru char(1) COMMENT 'codigo status grupo'
,cod_secao int COMMENT 'codigo secao'
,cod_scat int COMMENT 'codigo subcategoria'
,cod_categ_dmantec int COMMENT 'codigo categoria demandtec'
,cod_gru_rms int COMMENT 'codigo grupo rms'
,sgl_tpo_class_plu char(1) COMMENT 'sigla tipo classificacao produto'
,sgl_stpo_class_plu char(2) COMMENT 'sigla subtipo classificacao produto'
,num_seq int COMMENT 'sequencia que o registro aparece no arquivo'
,ind_ativ boolean COMMENT 'indicador ativo'
,dat_proces string COMMENT 'data processamento ingestao'
) STORED AS PARQUET 
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT' = 'Tabela de grupo')
;