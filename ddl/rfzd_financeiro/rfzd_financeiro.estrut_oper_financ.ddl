CREATE DATABASE IF NOT EXISTS rfzd_financeiro;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.estrut_oper_financ (
 COD_ESTRUT_FINANC_NVL_1	INT 	COMMENT "Codigo Estrutura Operacional Financeira Nivel 1"
,DESC_ESTRUT_FINANC_NVL_1   STRING  COMMENT "Descricao Estrutura Operacional Financeira Nivel 1"
,COD_ESTRUT_FINANC_NVL_2 	INT 	COMMENT "Codigo Estrutura Operacional Financeira Nivel 2"
,DESC_ESTRUT_FINANC_NVL_2   STRING  COMMENT "Descricao Estrutura Operacional Financeira Nivel 2"
,COD_ESTRUT_FINANC_NVL_3 	INT 	COMMENT "Codigo Estrutura Operacional Financeira Nivel 3"
,DESC_ESTRUT_FINANC_NVL_3   STRING  COMMENT "Descricao Estrutura Operacional Financeira Nivel 3"
,COD_ESTRUT_FINANC_NVL_4 	INT 	COMMENT "Codigo Estrutura Operacional Financeira Nivel 4"
,DESC_ESTRUT_FINANC_NVL_4   STRING  COMMENT "Descricao Estrutura Operacional Financeira Nivel 4"
,COD_ESTRUT_FINANC_NVL_5 	INT 	COMMENT "Codigo nivel 5. Filtro IN (40000, 40100, 40200, 40300) para 7.1 e IN (55000) para 4.1"
,DESC_ESTRUT_FINANC_NVL_5   STRING  COMMENT "Descricao Estrutura Operacional Financeira Nivel 5. Filtro IN (40000, 40100, 40200, 40300) para 7.1 e IN (55000) para 4.1"
,COD_ESTRUT_FINANC_NVL_6 	INT 	COMMENT "Codigo Estrutura Operacional Financeira Nivel 6"
,DESC_ESTRUT_FINANC_NVL_6   STRING  COMMENT "Descricao Estrutura Operacional Financeira Nivel 6"
,COD_ESTRUT_FINANC_NVL_7 	INT 	COMMENT "Codigo Estrutura Operacional Financeira Nivel 7"
,DESC_ESTRUT_FINANC_NVL_7   STRING  COMMENT "Descricao Estrutura Operacional Financeira Nivel 7"
,COD_ESTRUT_FINANC_NVL_8 	INT 	COMMENT "Codigo Estrutura Operacional Financeira Nivel 8"
,DESC_ESTRUT_FINANC_NVL_8   STRING  COMMENT "Descricao Estrutura Operacional Financeira Nivel 8"
,SGL_UF_LOJA  				STRING  COMMENT "Sigla da Unidade da Federação da Loja"
) PARTITIONED BY (DT_PROCESSAMENTO STRING COMMENT "Data da ingestao")
STORED AS PARQUET 
TBLPROPERTIES ("parquet.compression"="SNAPPY", "COMMENT" = "Tabela com visao da Estrutura Operacional 7.1 e 4.1")
;