CREATE DATABASE IF NOT EXISTS rwzd_ac01_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac01_hist.ext_td_ac01_dsdpm170_tsubgr17(
	  ind_acao string COMMENT 'Indicador de Acao I- INSERT, D- DELETE, U- UPDATE', 
	  cod_grupo string COMMENT 'Codigo do grupo', 
	  cod_subgrupo string COMMENT 'Codigo do subgrupo', 
	  ind_planogramavel string COMMENT 'Indica se produtos do subgrupo sao parte do planograma', 
	  nome_sub_grupo string COMMENT 'Nome do subgrupo', 
	  cod_status string COMMENT 'Indicador de status do subgrupo', 
	  cod_secao string COMMENT 'Codigo da Secao', 
	  num_seq string  COMMENT 	'Numero Sequencial do Registro dentro do Arquivo'
	  ) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao' )
	ROW FORMAT 
	DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm170_tsubgr17'	  
	TBLPROPERTIES ('parquet.compression'='SNAPPY','COMMENT'='Tabela de subgrupos realiza a ingestao diaria de dados inseridos alterados ou delatados 
	tendo como origem a view SC-TAB-SUB-GRUPO-Y2K'); 
