CREATE DATABASE IF NOT EXISTS rwzd_amelia;
CREATE TABLE IF NOT EXISTS rwzd_amelia.area_band (
 cod_area_band		String Comment 'Codigo Area Bandeira'
,cod_cep_ini		String Comment 'Codigo CEP Inicial'
,cod_cep_fim		String Comment 'Codigo CEP Final'
,nom_area_band		String Comment 'Nome Area Bandeira'
,dt_processamento	String Comment 'Data de Processamento'
)	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_amelia/area_band/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com dados de area/bandeira. Orgiem: Amelia')
;