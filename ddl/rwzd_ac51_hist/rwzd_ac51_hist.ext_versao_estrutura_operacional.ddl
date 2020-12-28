CREATE DATABASE IF NOT EXISTS rwzd_ac51_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac51_hist.ext_versao_estrutura_operacional (
  cod_tipo_estr_oper string ,
  idt_versao string ,
  dat_cadastro string,
  dat_liberacao string,
  dat_inic_vige string,
  dat_fim_vige string,
  cod_usu_atua string,
  dat_ult_atua string   
) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao')
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/rwzd_ac51_hist/ext_versao_estrutura_operacional"
TBLPROPERTIES ("parquet.compression"="SNAPPY");
