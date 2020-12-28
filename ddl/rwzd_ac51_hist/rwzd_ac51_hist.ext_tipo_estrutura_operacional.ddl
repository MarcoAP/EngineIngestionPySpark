CREATE DATABASE IF NOT EXISTS rwzd_ac51_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac51_hist.ext_tipo_estrutura_operacional (
  cod_tipo_estr_oper string,
  cod_area_assoc_estr string,
  cod_classif_tipo_estr string,
  nome_tipo_estr_oper string,
  ind_tipo_estr_princ string,
  ind_ativo string,
  ind_exportacao string,
  cod_tipo_fechamento string,
  cod_usu_atua string,
  dat_ult_atua string,
  num_nivel_incl_loja string          
) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao')
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/rwzd_ac51_hist/ext_tipo_estrutura_operacional"
TBLPROPERTIES ("parquet.compression"="SNAPPY");