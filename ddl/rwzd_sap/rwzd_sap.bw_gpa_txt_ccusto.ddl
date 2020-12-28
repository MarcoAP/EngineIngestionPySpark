CREATE DATABASE IF NOT EXISTS rwzd_sap;
CREATE TABLE IF NOT EXISTS rwzd_sap.bw_gpa_txt_ccusto(
desc_area_contab_custo STRING COMMENT 'Area Contabilidade e Custos - (0CO_AREA)',
cod_centro_custo STRING COMMENT 'Codigo do Centro de custo - (0COSTCENTER)',
cod_idioma STRING COMMENT 'Codigo de idioma - (0LANGU)',
dat_valid_fim STRING COMMENT 'Data de validade final - (0DATETO)',
dat_valid_ini STRING COMMENT 'Data de validade inicial - (0DATEFROM)',
txt_centro_custo_resum STRING COMMENT 'Texto centro de custo resumido - (0TXTSH)',
txt_centro_custo_med STRING COMMENT 'Texto centro de custo medio - (0TXTMD)',
dt_processamento STRING COMMENT 'Data do processamento'
)
PARTITIONED BY (ano_processamento STRING COMMENT 'ano da ingestao')
STORED AS PARQUET 
LOCATION  '/gpa/rawzone/rwzd_sap/bw_gpa_txt_ccusto/'
TBLPROPERTIES ('parquet.compression'='SNAPPY','COMMENT' = 'tabela de textos para centro de custos');
