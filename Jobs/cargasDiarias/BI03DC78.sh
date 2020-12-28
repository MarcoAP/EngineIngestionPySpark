#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
source /projetos/plfinanceiro/bin/environment_variables.sh
impala_shell_ip=brgpalnxslp006.gpa.sl:21000

spark-submit \
--master yarn \
--name "[Squad FI] Motor de ingestao Diario - AC34 Senhas - BI03DC78.sh" \
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
/projetos/plfinanceiro/bin/CoreFI/Main.py -mainframe ext_td_ac34_ac34rd55_senhas01

impala-shell -k -i $impala_shell_ip -B -q "INVALIDATE METADATA rwzd_ac34.ext_td_ac34_ac34rd55_senhas01;"