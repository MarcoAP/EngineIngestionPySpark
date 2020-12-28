#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
source /projetos/plfinanceiro/bin/environment_variables.sh
impala_shell_ip=brgpalnxslp006.gpa.sl:21000

spark-submit \
--class mvp.packageMain \
--master yarn \
--name "[Squad FI] Motor de Ingestao Diario - SIAC saccomprovante" \
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
--jars /projetos/plfinanceiro/bin/Libs/kudu-spark_2.10-1.5.0.jar /projetos/plfinanceiro/bin/MotorScala/dailySiacLoad/target/scala-2.10/dailysiacload_2.10-2.1.jar 12

impala-shell -k -i ${impala_shell_ip} -q "INVALIDATE METADATA siac_store_hist.ext_saccomprovante;"