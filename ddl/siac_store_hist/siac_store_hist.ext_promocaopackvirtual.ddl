CREATE DATABASE IF NOT EXISTS siac_store_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS siac_store_hist.ext_promocaopackvirtual (
  codloja STRING,
  numterminal STRING,
  numcontadorreiniciooperacao STRING,
  numseqoperacaoentrada STRING,
  numseqoperacao STRING,
  iddetpromocao STRING,
  codsecao STRING,
  valordesc STRING,
  codlegendatrib STRING,
  flagprodutogratis STRING,
  qtdegratis STRING,
  codagrupamento STRING,
  codformaaplicacao STRING,
  qtdeitensformampack STRING,
  qtdeconjuntosformados STRING,
  dt_processamento STRING COMMENT 'data de ingestao'
  ) PARTITIONED BY (datamovto STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;"
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/siac_store_hist/ext_promocaopackvirtual"
TBLPROPERTIES ("parquet.compression"="SNAPPY");