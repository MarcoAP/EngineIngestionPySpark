CREATE DATABASE IF NOT EXISTS rwzd_ac51_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac51_hist.ext_estrutura_operacional (
  cod_tipo_estr_oper string,
  cod_estr_oper string,             
  idt_versao string,             
  nome_estr_oper string,      
  num_nive_estr_oper string,             
  num_seq_apres string,             
  cod_ucbd string,             
  cod_tipo_estr_oper_pai string,            
  cod_estr_oper_pai string,             
  idt_versao_pai string,            
  cod_usu_atua string,        
  dat_ult_atua string             
) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao')
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/rwzd_ac51_hist/ext_estrutura_operacional"
TBLPROPERTIES ("parquet.compression"="SNAPPY");