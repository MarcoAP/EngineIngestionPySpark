CREATE DATABASE IF NOT EXISTS rwzd_ac01_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac01_hist.ext_td_ac01_dsdpm180_tdepat18 (
	ind_acao string		COMMENT 'Indicador de Acao I- INSERT, D- DELETE, U- UPDATE',
	cod_depto string 		COMMENT 'Codigo do departamento',
	txt_descricao string	COMMENT 'Nome do departamento',
	num_seq	 string 		COMMENT 	'Numero Sequencial do Registro dentro do Arquivo'
	) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao')
	ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION "/gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm180_tdepat18"
	TBLPROPERTIES ("parquet.compression"="SNAPPY",'COMMENT'='Identifica os departamentos de produtos do GPA origem SC-TAB-DEPTO')
	;