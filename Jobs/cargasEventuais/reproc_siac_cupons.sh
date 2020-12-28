#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
echo -e "########## Dropando tabela de cupons ##########"
hive -e "DROP TABLE IF EXISTS siac_store_hist.ext_cupons;"
echo -e "########## Removendo arquivos de cupons ##########"
hdfs dfs -rm -r -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/*
echo -e "########## Criando tabela de cupons ##########"
hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS siac_store_hist.ext_cupons (   codloja STRING,    numterminal STRING,   numcontadorreiniciooperacao STRING,   numseqoperacaoentrada STRING,   numseqoperacao STRING,   codtipocupom STRING,   codtipovenda STRING,   dthriniciovenda STRING,   dthrtotalizacao STRING,   dthrfimvenda STRING,   codcategoriacliente STRING,   codtipopbm STRING,   codclientepbm STRING,   nomeclientepbm STRING,   idprimariacliente STRING,   idsecundariacliente STRING,   codempresaconveniada STRING,   codorigempedido STRING,   codtipoareafiscal STRING,   qtdeitens STRING,   qtdeitemvenda STRING,   valorvenda STRING,   qtdeitemcancel STRING,   valorcancel STRING,   valordesc STRING,   valordescsubtotal STRING,   valorencargo STRING,   valortroco STRING,   valorcopay STRING,   qtdepontos STRING,   codstatuspontos STRING,   gtvenda STRING,   gtcancel STRING,   gtdesc STRING,   numcep STRING,   codoperador STRING,   codsupervisor STRING,   numidentclienteimpressa STRING,   descnomecliente STRING,   codprepedido STRING,   valordescsacolinha STRING,   numserieimpressora STRING,   numcontadorcuponsfiscais STRING,   contreducaoz STRING,   gtinivenda STRING,   gtinicancel STRING,   gtinidesc STRING,   dthrcancelamento STRING,   numtalao STRING,   numregistropromocoes STRING,   indicpagtounificado STRING,   tipocancelamento STRING,   indicacrescimorateio STRING,   codsupervvendalimite STRING,   tipodoccliente STRING,   numdoccliente STRING,   chavecupomfiscaleletronico STRING,   numcupomfiscaleletronico STRING,   numseriesatfiscal STRING,   numnfce STRING,   numserienfce STRING,   modoemissao STRING,   numprotocolo STRING,   modelodocfiscal STRING,   tipopagamento STRING,   valtottribfed STRING,   valtottribest STRING,   valtottribmun STRING,   cpfassociadonetpoints STRING,   indictementregadomicilio STRING,   valtotfrete STRING,   respofertacartaofic STRING,   codstatusnfegatewaysap STRING,   valordescconvertidoembonus STRING,   valorbonusrecarga STRING,   taxabonusrecarga STRING,   dthrinsercao STRING,   valortotalfcp STRING,   valortotalfcpsubsttribret STRING,   aliquotafcpsubsttribret STRING,   valoricmsfcpsubsttribret STRING,   codmunicipiofg STRING,   codlistaservico STRING,   indicissnfce STRING,   indicincentivofiscal STRING,   dt_processamento STRING COMMENT 'data de ingestao'    ) PARTITIONED BY (datamovto STRING)  STORED AS PARQUET LOCATION '/gpa/rawzone/stg/siac_store_hist/ext_cupons' TBLPROPERTIES ('parquet.compression'='SNAPPY');"

echo -e "########## Carregando dados de cupons ##########"
spark-submit \
--master yarn \
--name "[Squad FI] Motor de ingestao" \
--executor-memory 20G \
--executor-cores 4 \
--num-executors 9 \
--conf "spark.dynamicAllocation.enabled=true" \
--conf "spark.dynamicAllocation.initialExecutors=3" \
--conf "spark.dynamicAllocation.minExecutors=3" \
--conf "spark.dynamicAllocation.maxExecutors=9" \
--conf "spark.yarn.driver.memoryOverhead=2048" \
--conf "spark.yarn.executor.memoryOverhead=2048" \
--conf "spark.yarn.am.memoryOverhead=2048" \
--conf "spark.io.compression.codec=snappy" \
--conf "spark.rdd.compress=true" \
--conf "spark.sql.parquet.compression.codec=snappy" \
--conf "spark.driver.allowMultipleContexts=true" \
--conf "spark.eventLog.enabled=true" \
--conf "spark.hive.exec.dynamic.partition.key=hive.exec.dynamic.partition" \
--conf "spark.hive.exec.dynamic.partition.value=true" \
--conf "spark.hive.exec.dynamic.partition.mode.key=hive.exec.dynamic.partition.mode" \
--conf "spark.hive.exec.dynamic.partition.mode.value=nonstrict" \
--conf "spark.authenticate.secret=none" \
--queue "SquadFI" \
--jars /projetos/plfinanceiro/bin/Libs/commons-csv-1.2.jar,/projetos/plfinanceiro/bin/Libs/spark-csv_2.11-1.5.0.jar /projetos/plfinanceiro/bin//CoreFI/Main.py -sas