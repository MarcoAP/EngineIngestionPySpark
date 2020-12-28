CREATE DATABASE IF NOT EXISTS rwzd_ac51_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac51_hist.ext_area_tipo_estr (
  cod_area_assoc_estr string,
  nome_area_assoc_estr string,
  cod_usu_atua string,
  dat_ult_atua string       
) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao')
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/rwzd_ac51_hist/ext_area_tipo_estr"
TBLPROPERTIES ("parquet.compression"="SNAPPY");