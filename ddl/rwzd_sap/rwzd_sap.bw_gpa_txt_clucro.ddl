CREATE DATABASE IF NOT EXISTS rwzd_sap;
CREATE TABLE IF NOT EXISTS rwzd_sap.bw_gpa_txt_clucro(
desc_area_contab_custo STRING COMMENT 'Area Contabilidade e Custos - (0CO_AREA)',
cod_centro_lucro STRING COMMENT 'Codigo do Centro de lucro - (0PROFIT_CTR)',
cod_idioma STRING COMMENT 'Codigo de idioma - (0LANGU)',
dat_valid_fim STRING COMMENT 'Data de validade final - (0DATETO)',
dat_valid_ini STRING COMMENT 'Data de Validade inicial - (0DATEFROM)',
txt_centro_lucro_resum STRING COMMENT 'Texto centro de lucro resumido - (0TXTSH)',
txt_centro_lucro_medio STRING COMMENT 'Texto centro de lucro medio - (0TXTMD)',
dt_processamento STRING COMMENT 'Data do processamento'
)
PARTITIONED BY (ano_processamento STRING COMMENT 'ano da ingestao')
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  "/gpa/rawzone/rwzd_sap/bw_gpa_txt_clucro/"
TBLPROPERTIES ("parquet.compression"="SNAPPY",'COMMENT' = 'tabela de textos para centro de lucro');