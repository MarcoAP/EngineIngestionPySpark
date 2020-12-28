CREATE DATABASE IF NOT EXISTS siac_store_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS siac_store_hist.ext_promocao (
  codloja STRING,
  numterminal STRING,
  numcontadorreiniciooperacao STRING,
  numseqoperacaoentrada STRING,
  numseqoperacao STRING,
  iddetpromocao STRING,
  codtipopromocao STRING,
  codtipocodigopromocao STRING,
  codpromocao STRING,
  codinternoproduto STRING,
  codvenda STRING,
  codcategoriaefetivada STRING,
  qtdeitemvenda STRING,
  valorprecounitario STRING,
  dthrpromocao STRING,
  codoperador STRING,
  descconvertidobonusrecarga STRING,
  dt_processamento STRING COMMENT 'data de ingestao'
  ) PARTITIONED BY (datamovto STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;"
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/siac_store_hist/ext_promocao"
TBLPROPERTIES ("parquet.compression"="SNAPPY");