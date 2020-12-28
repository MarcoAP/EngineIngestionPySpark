#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
source /projetos/plfinanceiro/bin/environment_variables.sh
impala_shell_ip=brgpalnxslp006.gpa.sl:21000

#data de inicio da carga d -30 : PROCESSAR 30 dias corridos
dt=$(date -d '-30 day' '+%Y-%m-%d')
#data final de processamento: PROCESSAR ATE D-1
dt_partition=$(date -d '-1 day' '+%Y-%m-%d')
year_drop=$(date -d '-5 year' '+%Y')
dt_expurgo=${year_drop}"-12-31"
log=/projetos/plfinanceiro/log/plfinanceiro.log

spark-submit \
--master yarn \
--name "[Squad FI] - JOB BI03DC52 - Motor de ingestao Diario - TSZD_FINANCEIRO.APURACAO_MARGEM_VENDA" \
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
/projetos/plfinanceiro/bin/Datalake/Refined/RefinedMain.py -insert apur_margem_venda $dt_partition $dt


status_carga=$?
if [ ${status_carga} -eq 0 ];
then
echo -e "\n Iniciando Expurgo datamovto <= ${dt_expurgo}" >> $log
spark-submit \
--master yarn \
--name "[Squad FI] - JOB BI03DC52 - Motor de ingestao Diario - EXPURGO TSZD_FINANCEIRO.APURACAO_MARGEM_VENDA" \
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
/projetos/plfinanceiro/bin/Datalake/Refined/RefinedMain.py -expurgo apur_margem_venda $dt_expurgo
status_expurgo=$?
fi


if [ ${status_expurgo} -eq 0 ];
then
    echo -e "\n Expurgo realizado com sucesso" >> $log
else
    echo -e "\n Erro ao realizar expurgo de dados anteriores a 3 anos " >> $log
    echo -e "\n Status da carga: ${status_expurgo} " >> $log
fi

impala-shell -k -i $impala_shell_ip -B -q "INVALIDATE METADATA TSZD_FINANCEIRO.APURACAO_MARGEM_VENDA;"