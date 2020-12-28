CREATE DATABASE IF NOT EXISTS rwzd_amelia;
CREATE TABLE IF NOT EXISTS rwzd_amelia.tipos_comp_ecommerce (
 cod_tipo_compra		String Comment 'Codigo do Tipo da Compra'
,desc_tipo_compra		String Comment 'Descricao do Tipo da Conta'
,dt_processamento		String Comment 'Data de processamento'
)	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_amelia/tipos_comp_ecommerce/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Tabela com Dimensao dos Tipos de Compra. Orgiem: Amelia')
;
