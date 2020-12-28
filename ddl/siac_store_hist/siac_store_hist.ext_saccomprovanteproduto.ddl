CREATE DATABASE IF NOT EXISTS siac_store_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS siac_store_hist.ext_saccomprovanteproduto (
  codloja STRING,
  iddocumento STRING,
  numitem STRING,
  codinternoproduto STRING,
  codvendaproduto STRING,
  descproduto STRING,
  descunidade STRING,
  descembalagem STRING,
  qtdeitem STRING,
  valorprecovendaunitplu STRING,
  valorprecocustounitplu STRING,
  valordesconto STRING,
  valortotal STRING,
  codlegendatrib STRING,
  valoraliquotatrib STRING,
  codsituacaotributaria STRING,
  codsecao STRING,
  codgrupo STRING,
  codsubgrupo STRING,
  codvendedor STRING,
  codreserva STRING,
  indicdevolvidonaloja STRING,
  valorpis STRING,
  valorcofins STRING,
  valoraliquotapis STRING,
  valoraliquotacofins STRING,
  dt_processamento STRING COMMENT 'data de ingestao' 
  ) PARTITIONED BY (dtemissao STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;"
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/siac_store_hist/ext_saccomprovanteproduto"
TBLPROPERTIES ("parquet.compression"="SNAPPY");