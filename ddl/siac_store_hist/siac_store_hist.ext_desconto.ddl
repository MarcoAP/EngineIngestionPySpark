CREATE DATABASE IF NOT EXISTS siac_store_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS siac_store_hist.ext_desconto (
  codloja STRING,
  numterminal STRING,
  numcontadorreiniciooperacao STRING,
  numseqoperacaoentrada STRING,
  numseqoperacao STRING,
  iddetdesconto STRING,
  codtipodesconto STRING,
  codtipocodigopromocao STRING,
  codpromocao STRING,
  codinternoproduto STRING,
  codvenda STRING,
  qtdeitemvenda STRING,
  valorprecounitario STRING,
  dthrdesconto STRING,
  codoperador STRING,
  descconvertidobonusrecarga STRING,
  dt_processamento STRING COMMENT 'data de ingestao'
  ) PARTITIONED BY (datamovto STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;"
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/siac_store_hist/ext_desconto"
TBLPROPERTIES ("parquet.compression"="SNAPPY");