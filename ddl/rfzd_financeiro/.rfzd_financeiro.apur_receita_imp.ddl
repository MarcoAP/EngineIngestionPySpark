CREATE TABLE IF NOT EXISTS rfzd_financeiro.apur_receita_imp(
val_receita_imp 	DOUBLE COMMENT "Valor de receita ou impostos"
,dat_lancto 		STRING COMMENT "Data de lancamento "
,cod_loja 			INT COMMENT "Codigo da Loja"
,num_cta_ac51		INT  COMMENT "Conta AC51"
,dt_processamento 	STRING COMMENT 'Data do processamento'
) STORED AS PARQUET 
LOCATION  "/gpa/refinedzone/rfzd_financeiro/apur_receita_imp/"
TBLPROPERTIES ("parquet.compression"="SNAPPY", "COMMENT" = "TABELA DE APURACAO DE DE RECEITA DE SERVICOS E IMPOSTOS")
;