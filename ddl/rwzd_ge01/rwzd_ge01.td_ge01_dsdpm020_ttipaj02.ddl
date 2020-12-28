CREATE TABLE IF NOT EXISTS rwzd_ge01.td_ge01_dsdpm020_ttipaj02 (
 ind_acao			STRING COMMENT 'Indicador de acao'
,cod_campo			STRING COMMENT 'Codigo do campo'
,desc_campo			STRING COMMENT 'Descricao do campo'
,num_seq			STRING COMMENT 'Numero sequencial'
,dt_processamento	STRING COMMENT 'Data de Processamento'
)	PARTITIONED BY (ano_processamento STRING COMMENT 'ano de processamento')
	ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_ge01/td_ge01_dsdpm020_ttipaj02/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com dados do tipo de ajuste de estoque. Orgiem: GE01')
;
