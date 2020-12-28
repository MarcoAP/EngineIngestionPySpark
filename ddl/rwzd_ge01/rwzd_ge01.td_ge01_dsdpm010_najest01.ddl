CREATE TABLE IF NOT EXISTS rwzd_ge01.td_ge01_dsdpm010_najest01 (
 num_seq			STRING COMMENT 'Numero sequencial'
,cod_loja			STRING COMMENT 'Codigo da loja'
,cod_plu			STRING COMMENT 'Codigo do produto'
,cod_mot_ajust		STRING COMMENT 'Codigo de motivo do ajuste'
,cod_ajust			STRING COMMENT 'Codigo do ajuste'
,dat_mov			STRING COMMENT 'Data do movimento'
,dat_atua			STRING COMMENT 'Data da atualizacao'
,qtd_ajust			STRING COMMENT 'Quantidade de ajuste'
,hor_ajust			STRING COMMENT 'Hora do ajuste'
,dt_processamento	STRING COMMENT 'Data de Processamento'
)	PARTITIONED BY (ano_processamento STRING COMMENT 'ano de processamento')
	ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_ge01/td_ge01_dsdpm010_najest01/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com dados de ajuste de estoque. Orgiem: GE01')
;
