CREATE DATABASE IF NOT EXISTS rwzd_vb;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_vb.ext_mot_alt_preco (
	cod_mot_alter string COMMENT 'codigo do motivo de alteracao preco'
	desc_mot_alter string COMMENT 'descricao do motivo de alteracao preco'
	) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao')
	ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION "/gpa/rawzone/stg/rwzd_vb/ext_mot_alt_preco"
	TBLPROPERTIES ("parquet.compression"="SNAPPY",'COMMENT'='Tabela que identifica os motivos de alteracao de precos de produtos')
	;