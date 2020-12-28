CREATE TABLE IF NOT EXISTS rwzd_sap.zfii064_gca3_con(
	 ano_mes_lancto	STRING  COMMENT "Ano mes de lancamento"
	,cod_loja 		STRING 	COMMENT "Código da Loja"
	,dia_lancto     STRING  COMMENT "Dia do lancamento"
	,conta_ac51		STRING  COMMENT "Conta AC51"
	,val_receita    STRING  COMMENT "Valor da Receita"
	,dt_processamento STRING COMMENT 'Data do processamento'
)
STORED AS PARQUET 
LOCATION  "/gpa/rawzone/rwzd_sap/zfii064_gca3_con/"
TBLPROPERTIES ("parquet.compression"="SNAPPY", "COMMENT" = "Tabela com informacoes de receita de servicos + impostos")
;