CREATE DATABASE IF NOT EXISTS rwzd_sap;
CREATE TABLE IF NOT EXISTS rwzd_sap.bw_gpa_txt_contas(
desc_plano_cta STRING COMMENT 'Descricao do Planos de Contas - (CHRT_ACCTS)',
cod_cta_rz STRING COMMENT 'Codigo da Conta Razao - (GL_ACCOUNT)',
cod_idioma STRING COMMENT 'Codigo de Idioma - (LANGU)',
txt_cta_resum STRING COMMENT 'Descricao da conta resumida - (TXTSH)',
txt_cta_compl STRING COMMENT 'Descricao da conta completa - (TXTLG)',
dt_processamento STRING COMMENT 'Data do processamento'
) 
PARTITIONED BY (ano_processamento STRING COMMENT 'ano da ingestao' )
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  "/gpa/rawzone/rwzd_sap/bw_gpa_txt_contas/"
TBLPROPERTIES ("parquet.compression"="SNAPPY",'COMMENT' = 'tabela de textos para Contas');