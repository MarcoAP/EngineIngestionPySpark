#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
# transfere arquivos para HDFS
hdfs dfs -put -f /projetos/plfinanceiro/data/input/siac_store_hist/ext_cuponsitens/* /gpa/rawzone/stg/siac_store_hist/input/ext_cuponsitens/
hdfs dfs -put -f /projetos/plfinanceiro/data/input/siac_store_hist/ext_cupons/* /gpa/rawzone/stg/siac_store_hist/input/ext_cupons/
hdfs dfs -put -f /projetos/plfinanceiro/data/input/siac_store_hist/ext_descbonusfidelidade/* /gpa/rawzone/stg/siac_store_hist/input/ext_descbonusfidelidade/

# carga estrutura mesrcadologica historico e siac historico do Teradata
spark-submit \
--master yarn \
--name "[Squad FI] Motor de ingestão - Historico Teradata: estrutura mercadologia e SIAC " \
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
--jars commons-csv-1.2.jar,spark-csv_2.11-1.5.0.jar CoreFI/Main.py -sas
