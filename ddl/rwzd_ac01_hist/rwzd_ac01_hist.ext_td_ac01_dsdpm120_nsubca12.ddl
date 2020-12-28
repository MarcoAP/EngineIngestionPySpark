CREATE DATABASE IF NOT EXISTS rwzd_ac01_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac01_hist.ext_td_ac01_dsdpm120_nsubca12 (
	  cod_comprador string COMMENT 'codigo da subcategoria' , 
	  nome_comprador string COMMENT 'nome da subcategoria', 
	  cod_nivel_superior string COMMENT 'codigo do nivel superior', 
	  cod_divisao string
	  ) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao' )
	ROW FORMAT 
	DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm120_nsubca12' 
	TBLPROPERTIES ('parquet.compression'='SNAPPY','COMMENT'='tabela de subcategoria realiza a ingestao com base na origem SC_TAB_COMPRADOR')
	;