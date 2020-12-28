CREATE DATABASE IF NOT EXISTS rfzd_financeiro;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.estrut_oper_gerenc_gca3 (
 COD_ESTRUT_GERENC_NVL_1 	INT 	COMMENT "Codigo Estrutura Operacional Gerencial GCA3 Nivel 1"
,DESC_ESTRUT_GERENC_NVL_1   STRING  COMMENT "Descricao Estrutura Operacional Gerencial GCA3 Nivel 1"
,COD_ESTRUT_GERENC_NVL_2 	INT 	COMMENT "Codigo Estrutura Operacional Gerencial GCA3 Nivel 2"
,DESC_ESTRUT_GERENC_NVL_2   STRING  COMMENT "Descricao Estrutura Operacional Gerencial GCA3 Nivel 2"
,COD_ESTRUT_GERENC_NVL_3 	INT 	COMMENT "Codigo Estrutura Operacional Gerencial GCA3 Nivel 3"
,DESC_ESTRUT_GERENC_NVL_3   STRING  COMMENT "Descricao Estrutura Operacional Gerencial GCA3 Nivel 3"
,COD_ESTRUT_GERENC_NVL_4 	INT 	COMMENT "Codigo Estrutura Operacional Gerencial GCA3 Nivel 4"
,DESC_ESTRUT_GERENC_NVL_4   STRING  COMMENT "Descricao Estrutura Operacional Gerencial GCA3 Nivel 4"
,COD_ESTRUT_GERENC_NVL_5 	INT 	COMMENT "Codigo Estrutura Operacional Gerencial GCA3 Nivel 5"
,DESC_ESTRUT_GERENC_NVL_5   STRING  COMMENT "Descricao Estrutura Operacional Gerencial GCA3 Nivel 5"
,COD_ESTRUT_GERENC_NVL_6 	INT 	COMMENT "Codigo Estrutura Operacional Gerencial GCA3 Nivel 6"
,DESC_ESTRUT_GERENC_NVL_6   STRING  COMMENT "Descricao Estrutura Operacional Gerencial GCA3 Nivel 6"
,COD_ESTRUT_GERENC_NVL_7 	INT 	COMMENT "Codigo Estrutura Operacional Gerencial GCA3 Nivel 7"
,DESC_ESTRUT_GERENC_NVL_7   STRING  COMMENT "Descricao Estrutura Operacional Gerencial GCA3 Nivel 7"
) PARTITIONED BY (DT_PROCESSAMENTO STRING COMMENT "Data da ingestao")
STORED AS PARQUET 
TBLPROPERTIES ("parquet.compression"="SNAPPY", "COMMENT" = "Tabela com visao da Estrutura Operacional 1.2")
;
