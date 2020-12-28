CREATE DATABASE IF NOT EXISTS rwzd_sap;
CREATE TABLE IF NOT EXISTS rwzd_sap.bw_gpa_txt_clscusto(
desc_area_contab_custo STRING COMMENT 'Area Contabilidade e Custos - (0CO_AREA)',
cod_classe_custo STRING COMMENT 'Codigo da classe de custo - (0COSTELMNT)',
cod_idioma STRING COMMENT 'Codigo de idioma - (0LANGU)',
dat_valid_fim STRING COMMENT 'Data de validade final - (0DATETO)',
dat_valid_ini STRING COMMENT 'Data de Validade inicial - (0DATEFROM)',
txt_classe_custo_resum STRING COMMENT 'Texto classe de custo resumido - (0TXTSH)',
txt_classe_custo_medio STRING COMMENT 'Texto classe de custo breve - (0TXTMD)',
dt_processamento STRING COMMENT 'Data do processamento'
) 
PARTITIONED BY (ano_processamento STRING COMMENT 'ano da ingestao')
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  "/gpa/rawzone/rwzd_sap/bw_gpa_txt_clscusto/"
TBLPROPERTIES ("parquet.compression"="SNAPPY",'COMMENT' = 'tabela de textos para classes de custos temporaria');