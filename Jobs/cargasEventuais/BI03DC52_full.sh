#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
log=/projetos/plfinanceiro/log/plfinanceiro.log
impala_conn=brgpalnxslp006.gpa.sl
#Inicio
dt=("2015-01-01" "2015-04-01" "2015-07-01" "2015-10-01" "2016-01-01" "2016-04-01" "2016-07-01" "2016-10-01" "2017-01-01" "2017-04-01" "2017-07-01" "2017-10-01" "2018-01-01" "2018-04-01" "2019-01-01" "2019-04-01")
#Fim
dt_partition=("2015-03-31" "2015-06-30" "2015-09-30" "2015-12-31" "2016-03-31" "2016-06-30" "2016-09-30" "2016-12-31" "2017-03-31" "2017-06-30" "2017-09-30" "2017-12-31" "2018-03-31" "2018-06-30" "2019-03-31" "2019-05-28")

i=0
while [ 15 -ge ${i} ]; do
    echo -e "\n############### NOVA EXECUCAO HQL MARGEM FULL###############" >> $log
    echo -e "\nINICIO DA CARGA DE: ${dt[$i]} ATE: ${dt_partition[$i]} $(date +'%Y-%m-%d %H:%M:%S')" >> $log

    spark-submit \
    --master yarn \
    --name "[Squad FI] - JOB XXX - Motor de ingestao FULL - TSZD_FINANCEIRO.APURACAO_MARGEM_VENDA" \
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
    /projetos/plfinanceiro/bin/Datalake/Refined/RefinedMain.py -insert apur_margem_venda_full ${dt_partition[$i]} ${dt[$i]}
    i=$(expr $i + 1)
done

impala-shell -k -i $impala_shell_ip -B -q "INVALIDATE METADATA TSZD_FINANCEIRO.APURACAO_MARGEM_VENDA;"