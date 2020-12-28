CREATE DATABASE IF NOT EXISTS siac_store_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS siac_store_hist.ext_promocaodesc (
  codloja STRING,
  numterminal STRING,
  numcontadorreiniciooperacao STRING,
  numseqoperacaoentrada STRING,
  numseqoperacao STRING,
  iddetpromocao STRING,
  valorprecounitariopromo STRING,
  valorbasedesc STRING,
  valorpercdesc STRING,
  valordescunitario STRING,
  valordesc STRING,
  codsupervisor STRING,
  codformaaplicacao STRING,
  numnsupdvgift STRING,
  numnsupdvfic STRING,
  codstatusbonusfic STRING,
  numseqcupomfic STRING,
  valorcompra STRING,
  valorcupomgift STRING,
  valorconfeccaocartao STRING,
  codstatusrecarga STRING,
  valorbonusfic STRING,
  codsecao STRING,
  codgrupo STRING,
  codsubgrupo STRING,
  dt_processamento STRING COMMENT 'data de ingestao'
  ) PARTITIONED BY (datamovto STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;"
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/siac_store_hist/ext_promocaodesc"
TBLPROPERTIES ("parquet.compression"="SNAPPY");