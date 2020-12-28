CREATE DATABASE IF NOT EXISTS RFZD_FINANCEIRO;
CREATE TABLE IF NOT EXISTS RFZD_FINANCEIRO.ESTRUT_OPER_ESCRIT_REGION (
 COD_ESTRUT_ESCRIT_NVL_1 	INT 	COMMENT "Codigo Estrutura Operacional Escritorio Regional Nivel 1 (estrutura operacional dentro AC51)"
,DESC_ESTRUT_ESCRIT_NVL_1	STRING  COMMENT "Descricao  Estrutura Operacional Escritorio Regional Nivel 1 (estrutura operacional dentro AC51)"
,COD_ESTRUT_ESCRIT_NVL_2 	INT 	COMMENT "Codigo  Estrutura Operacional Escritorio Regional Nivel 2 (estrutura operacional dentro AC51)"
,DESC_ESTRUT_ESCRIT_NVL_2	STRING  COMMENT "Descricao Estrutura Operacional Escritorio Regional Nivel 2 (estrutura operacional dentro AC51)"
,COD_ESTRUT_ESCRIT_NVL_3 	INT 	COMMENT "Codigo Estrutura Operacional Escritorio Regional Nivel 3 (estrutura operacional dentro AC51)"
,DESC_ESTRUT_ESCRIT_NVL_3	STRING  COMMENT "Descricao Estrutura Operacional Escritorio Regional Nivel 3 (estrutura operacional dentro AC51)"
,COD_ESTRUT_ESCRIT_NVL_4 	INT 	COMMENT "Codigo Estrutura Operacional Escritorio Regional Nivel 4 (estrutura operacional dentro AC51)"
,DESC_ESTRUT_ESCRIT_NVL_4	STRING  COMMENT "Descricao  Estrutura Operacional Escritorio Regional Nivel 4 (estrutura operacional dentro AC51)"
,COD_ESTRUT_ESCRIT_NVL_5 	INT 	COMMENT "Codigo  Estrutura Operacional Escritorio Regional Nivel 5 (estrutura operacional dentro AC51)"
,DESC_ESTRU_ESCRITT_NVL_5	STRING  COMMENT "Descricao Estrutura Operacional Escritorio Regional  Nivel 5 (estrutura operacional dentro AC51)"
,COD_ESTRUT_ESCRIT_NVL_6 	INT 	COMMENT "Codigo  Estrutura Operacional Escritorio Regional Nivel 6 (estrutura operacional dentro AC51)"
,DESC_ESTRUT_ESCRIT_NVL_6	STRING  COMMENT "Descricao  Estrutura Operacional Escritorio Regional Nivel 6 (estrutura operacional dentro AC51)"
) PARTITIONED BY (DT_PROCESSAMENTO STRING COMMENT "Data da ingestao")
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY", "COMMENT" = "Tabela com visao da Estrutura Operacional 2.2")
;