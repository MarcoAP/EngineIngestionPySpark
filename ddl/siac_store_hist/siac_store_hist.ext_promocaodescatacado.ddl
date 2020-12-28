CREATE DATABASE IF NOT EXISTS siac_store_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS siac_store_hist.ext_promocaodescatacado (
  codloja STRING,
  numterminal STRING,
  numcontadorreiniciooperacao STRING,
  numseqoperacaoentrada STRING,
  numseqoperacao STRING,
  iddetpromocao STRING,
  valordesc STRING,
  codsecao STRING,
  codtipopremio STRING,
  codtipoaplicacao STRING,
  codgrupo STRING,
  codlegendatrib STRING,
  dt_processamento STRING COMMENT 'data de ingestao'
  ) PARTITIONED BY (datamovto STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;"
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/siac_store_hist/ext_promocaodescatacado"
TBLPROPERTIES ("parquet.compression"="SNAPPY");