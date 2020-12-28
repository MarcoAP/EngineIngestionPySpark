CREATE DATABASE IF NOT EXISTS siac_store_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS siac_store_hist.ext_saccomprovante (
  codloja STRING Comment 'Codigo da Loja' , 
  iddocumento STRING Comment 'id do Documento',
  codtipodocumento STRING Comment 'Codigo tipo do documento',
  cupomcodloja STRING Comment 'Codigo da loja do cupom',
  cupomnumseqoperacao STRING Comment 'Numero sequencia Operacao cupom',
  qtdeitem STRING Comment 'Quantidade de item',
  valordesconto STRING Comment 'Valor desconto',
  valortotal STRING Comment 'Valor total',
  datamovtocupom STRING Comment 'data movimento do cupom',
  numdocumento STRING Comment 'Numero do documento',
  numterminalcupom STRING Comment 'numero do terminal do cupom',
  codtipodevolucao STRING Comment 'codigo do tipo de devolucao',
  descmotivotroca STRING Comment 'descricao do motivo de troca',
  numterminalutilizado STRING Comment 'numero do terminal utilizado',
  dtvalidade STRING Comment 'data de validade',
  codtipo STRING Comment 'codigo do tipo',
  desctipodevolucao STRING Comment 'descricao do tipo de devolucao',
  idcomprovantesituacao STRING Comment 'id comprovante',
  codsituacao STRING Comment 'codigo situacao',
  descidentificacaousuario STRING Comment 'desricao identificacao do usuario',
  nomeusuario STRING Comment 'nome do usuario',
  dtmovsituacao STRING Comment 'data movimento da situacao',
  numseqoperacaoutilizado STRING Comment 'numero sequecia da operacao utilizado',
  indicestorefanulado STRING,
  descmotivo STRING Comment 'descricao do motivo',
  dthremissaocomprovante STRING Comment 'data e hora de emissao do comprovante',
  codoperadorpdv STRING Comment 'codigo operador pdv',
  descconvertidobonusrecarga STRING ,
  dthrinsercao STRING Comment 'data e hora de insercao',
  dt_processamento STRING  COMMENT 'data de ingestao'  
  ) PARTITIONED BY (dtemissao STRING Comment 'data de emissao') ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;"
STORED AS PARQUET
LOCATION "/gpa/rawzone/stg/siac_store_hist/ext_saccomprovante"
TBLPROPERTIES ("parquet.compression"="SNAPPY");