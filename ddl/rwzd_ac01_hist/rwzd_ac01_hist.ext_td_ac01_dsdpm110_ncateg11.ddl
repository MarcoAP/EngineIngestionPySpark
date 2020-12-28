CREATE DATABASE IF NOT EXISTS rwzd_ac01_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac01_hist.ext_td_ac01_dsdpm110_ncateg11 (
	  cod_comprador string COMMENT 'codigo da categoria', 
	  nome_comprador string COMMENT 'nome da categoria', 
	  cod_nivel_superior string COMMENT 'codigo do nivel superior'
	  ) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao' )
	ROW FORMAT 
	DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm110_ncateg11'
	TBLPROPERTIES ('parquet.compression'='SNAPPY','COMMENT'='tabela de categoria realiza a ingestao com base na origem SC_TAB_COMPRADOR');
	