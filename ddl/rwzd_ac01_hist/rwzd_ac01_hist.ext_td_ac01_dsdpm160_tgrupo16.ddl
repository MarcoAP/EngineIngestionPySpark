CREATE DATABASE IF NOT EXISTS rwzd_ac01_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac01_hist.ext_td_ac01_dsdpm160_tgrupo16 (
	  ind_acao string COMMENT 'Indicador de Acao I- INSERT, D- DELETE, U- UPDATE', 
	  cod_secao string COMMENT 'Codigo da Secao', 
	  cod_subunidade string COMMENT	'Codigo da subcategoria', 
	  cod_grupo string COMMENT	'Codigo do grupo', 
	  nome_grupo string COMMENT	'Nome do grupo', 
	  cod_status string COMMENT	'Indicador de status do grupo.', 
	  cod_categoria_demandtec string COMMENT 'Codigo da categoria Demandtec', 
	  cod_grupo_rms string COMMENT 'Codigo do Grupo RMS - ERP', 
	  sigl_tipo_classific_prod string COMMENT	'Tipo de venda ao qual o subtipo esta vinculado.', 
	  sigl_subtipo_classific_prod string COMMENT 'Subtipo de venda.', 
	  num_seq string COMMENT 	'Numero Sequencial do Registro dentro do Arquivo'
	  ) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao' )
	ROW FORMAT 
	DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm160_tgrupo16'
	TBLPROPERTIES ('parquet.compression'='SNAPPY','COMMENT'='Tabela de grupos realiza a ingestao diaria de dados inseridos alterados ou delatados 
	tendo como origem a view SC-TAB-GRUPO-Y2K');