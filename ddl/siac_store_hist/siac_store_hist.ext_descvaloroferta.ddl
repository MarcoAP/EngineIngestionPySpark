CREATE DATABASE IF NOT EXISTS siac_store_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS siac_store_hist.ext_descvaloroferta (
  codloja STRING,
  numterminal STRING,
  numcontadorreiniciooperacao STRING,
  numseqoperacaoentrada STRING,
  numseqoperacao STRING,
  iddetdesconto STRING,
  valordesconto STRING,
  valoroferta STRING,
  dt_processamento STRING COMMENT 'data de ingestao'
  ) PARTITIONED BY (datamovto STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;"
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/siac_store_hist/ext_descvaloroferta"
TBLPROPERTIES ("parquet.compression"="SNAPPY");