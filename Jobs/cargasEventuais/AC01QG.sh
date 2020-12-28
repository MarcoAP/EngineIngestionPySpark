#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
source /projetos/plfinanceiro/bin/environment_variables.sh
#Paramentros para Producao-----------------------------------
#impala_conn=brgpalnxslp006.gpa.sl
hive_conn="jdbc:hive2://brgpalnxslp005.gpa.sl:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA;"
#------------------------------------------------------------
#------------------------------------------------------------
#Paramentros para Dev----------------------------------------
#impala_conn=brgpalnxslp006.gpa.sl
#hive_conn="jdbc:hive2://brgpalnxsld002.gpa.sl:10000/default;principal=hive/brgpalnxsld002.gpa.sl@HADOOP.DEV.GPA;"

dt_partition=$(date '+%Y-%m-%d')

echo -e "########## Removendo arquivos de produto ##########"
hdfs dfs -rm -r -skipTrash /gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm010_tprodu01/*
echo -e "########## Removendo Particao ##########"
beeline -u ${hive_conn} --silent=true  -e "ALTER TABLE rwzd_ac01_hist.ext_td_ac01_dsdpm010_tprodu01 DROP IF EXISTS PARTITION  (dt_processamento <='$dt_partition');"

#echo -e "########## Removendo arquivos de subgrupo ##########"
#hdfs dfs -rm -r -skipTrash /gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm170_tsubgr17/*
#echo -e "########## Removendo Particao ##########"
#beeline -u ${hive_conn} --silent=true  -e "ALTER TABLE rwzd_ac01_hist.ext_td_ac01_dsdpm170_tsubgr17 DROP IF EXISTS PARTITION  (dt_processamento <='$dt_partition');"

#echo -e "########## Removendo arquivos de grupo ##########"
#hdfs dfs -rm -r -skipTrash /gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm160_tgrupo16/*
#echo -e "########## Removendo Particao ##########"
#beeline -u ${hive_conn} --silent=true  -e "ALTER TABLE rwzd_ac01_hist.ext_td_ac01_dsdpm160_tgrupo16 DROP IF EXISTS PARTITION  (dt_processamento <='$dt_partition');"

#echo -e "########## Removendo arquivos de subcategoria ##########"
#hdfs dfs -rm -r -skipTrash /gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm120_nsubca12/*
#echo -e "########## Removendo Particao ##########"
#beeline -u ${hive_conn} --silent=true  -e "ALTER TABLE rwzd_ac01_hist.ext_td_ac01_dsdpm120_nsubca12 DROP IF EXISTS PARTITION  (dt_processamento <='$dt_partition');"

#echo -e "########## Removendo arquivos de categoria ##########"
#hdfs dfs -rm -r -skipTrash /gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm110_ncateg11/*
#echo -e "########## Removendo Particao ##########"
#beeline -u ${hive_conn} --silent=true  -e "ALTER TABLE rwzd_ac01_hist.ext_td_ac01_dsdpm110_ncateg11 DROP IF EXISTS PARTITION  (dt_processamento <='$dt_partition');"

#echo -e "########## Removendo arquivos de secao ##########"
#hdfs dfs -rm -r -skipTrash /gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm190_tsecao19/*
#echo -e "########## Removendo Particao ##########"
#beeline -u ${hive_conn} --silent=true  -e "ALTER TABLE rwzd_ac01_hist.ext_td_ac01_dsdpm190_tsecao19 DROP IF EXISTS PARTITION  (dt_processamento <='$dt_partition');"

#echo -e "########## Removendo arquivos de departamento ##########"
#hdfs dfs -rm -r -skipTrash /gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm180_tdepat18/*
#echo -e "########## Removendo Particao ##########"
#beeline -u ${hive_conn} --silent=true  -e "ALTER TABLE rwzd_ac01_hist.ext_td_ac01_dsdpm180_tdepat18 DROP IF EXISTS PARTITION  (dt_processamento <='$dt_partition');"

echo -e "########## Carregando dados da estrutura mercadologica ##########"
spark-submit \
--master yarn \
--name "[Squad FI] Motor de ingestao AC01 Teradata" \
--executor-memory 20G \
--num-executors 9 \
--executor-cores 4 \
--conf "spark.yarn.am.memoryOverhead=2048" \
--conf "spark.yarn.executor.memoryOverhead=2048" \
--conf "spark.dynamicAllocation.initialExecutors=9" \
--conf "spark.dynamicAllocation.minExecutors=9" \
--conf "spark.dynamicAllocation.maxExecutors=9" \
--queue "SquadFI" \
--jars /projetos/plfinanceiro/bin/Libs/commons-csv-1.2.jar,/projetos/plfinanceiro/bin/Libs/spark-csv_2.11-1.5.0.jar /projetos/plfinanceiro/bin/CoreFI/Main.py -teradata ac01
