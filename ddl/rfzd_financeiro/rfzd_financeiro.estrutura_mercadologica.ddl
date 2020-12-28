CREATE DATABASE IF NOT EXISTS rfzd_financeiro;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.estrutura_mercadologica (
 cod_plu INT COMMENT "Codigo de barra interno do GPA (plu)"
,desc_plu STRING COMMENT "Nome completo do produto"
,cod_subgrupo INT COMMENT "Codigo do subgrupo"
,desc_subgrupo STRING COMMENT "Descricao do subgrupo"
,cod_grupo INT COMMENT "Codigo do grupo"
,desc_grupo STRING COMMENT "Descricao do grupo"
,cod_subcategoria INT COMMENT "Codigo do subcategoria"
,desc_subcategoria STRING COMMENT "Descricao da subcategoria"
,cod_secao INT COMMENT "Codigo da secao"
,desc_secao STRING COMMENT "Descricao da secao"
,cod_categoria INT COMMENT "Codigo da categoria"
,desc_categoria STRING COMMENT "Descricao da categoria"
,cod_departamento INT COMMENT "Codigo do departamento"
,desc_departamento STRING COMMENT "Descricao do departamento"
,cod_divisao INT COMMENT "Codigo da divisao"
,desc_divisao STRING COMMENT "Descricao da divisao"
,ind_ativo BOOLEAN COMMENT "Indicador de registro ativo (true = ativo, false = inativo)"
) PARTITIONED BY (dt_processamento STRING COMMENT "Data da ingestao")
STORED AS PARQUET 
LOCATION '/gpa/refinedzone/rfzd_financeiro/estrutura_mercadologica' 
TBLPROPERTIES ("parquet.compression"="SNAPPY", "COMMENT" = "Tabela de estrutura mercadologica consolidada")
;