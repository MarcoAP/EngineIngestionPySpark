CREATE DATABASE IF NOT EXISTS rwzd_ac01_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac01_hist.ext_td_ac01_dsdpm190_tsecao19 (
	ind_acao string		COMMENT 'Indicador de Acao I- INSERT, D- DELETE, U- UPDATE',
	cod_secao string		COMMENT 'Código da Secao',
	cod_depto string 		COMMENT 'Código do departamento',
	nome_secao string 		COMMENT 'Nome da Secao',
	num_seq	 string 		COMMENT 'Numero Sequencial do Registro dentro do Arquivo'
	) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestão')
	ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION "/gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm190_tsecao19"
	TBLPROPERTIES ("parquet.compression"="SNAPPY",'COMMENT'='Identifica as secoes do GPA origem SC-TAB-SECAO-Y2K')
	;
