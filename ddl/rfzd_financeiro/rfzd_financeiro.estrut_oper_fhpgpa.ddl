CREATE DATABASE IF NOT EXISTS rfzd_financeiro;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.estrut_oper_fhpgpa (
 cod_estrut_geral	STRING COMMENT "Codigo Estrutura Geral"
,desc_estrut_geral	STRING COMMENT "Descricao Estrutura Geral"
,cod_empr_gru		STRING COMMENT "Codigo Empresas Grupo"
,desc_empr_gru		STRING COMMENT "Descricao Empresas Grupo"
,cod_empr_sgru		STRING COMMENT "Codigo Empresas Subgrupo"
,desc_empr_sgru		STRING COMMENT "Descricao Empresas Subgrupo"
,cod_bu				STRING COMMENT "Codigo BU"
,desc_bu			STRING COMMENT "Descricao BU"
,cod_band			STRING COMMENT "Codigo Bandeira"
,desc_band			STRING COMMENT "Descricao Bandeira"
,cod_regiao			STRING COMMENT "Codigo Regiao"
,desc_regiao		STRING COMMENT "Descricao Regiao"
,cod_region			STRING COMMENT "Codigo Regional"
,desc_region		STRING COMMENT "Descricao Regional"
,cod_loja			STRING COMMENT "Codigo Loja"
,desc_loja			STRING COMMENT "Descricao Loja"
,dt_processamento	STRING COMMENT 'Data de processamento'
) PARTITIONED BY (ano_processamento INT COMMENT 'ano da ingestao')
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY", "COMMENT" = "Tabela com visao da Estrutura Operacional SAP - FHPGPA")
;
