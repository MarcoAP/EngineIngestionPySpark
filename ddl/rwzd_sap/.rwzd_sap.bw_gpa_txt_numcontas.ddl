CREATE DATABASE IF NOT EXISTS rwzd_sap;
CREATE TABLE IF NOT EXISTS rwzd_sap.bw_gpa_txt_numcontas(
desc_plano_cta STRING COMMENT "Descricao do Planos de Contas - (Campo SAP: CHRT_ACCTS)"
,num_cta STRING COMMENT 'Numero da Conta- (Campo SAP: ACCOUNT)'
,cod_idioma STRING COMMENT 'Codigo de Idioma - (Campo SAP: LANGU)'
,desc_breve STRING COMMENT 'Descricao Breve - (Campo SAP: TXTSH)'
,desc_media STRING COMMENT 'Descricao Media - (Campo SAP: TXTMD)'
,dt_processamento STRING COMMENT 'Data de Processamento'
) 
PARTITIONED BY (mes_processamento STRING COMMENT 'mes de processamento')
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  "/gpa/rawzone/rwzd_sap/bw_gpa_txt_numcontas/"
TBLPROPERTIES ("parquet.compression"="SNAPPY",'COMMENT' = 'tabela de num_conta SAP');