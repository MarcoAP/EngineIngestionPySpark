#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
impala_shell=brgpalnxslp006.gpa.sl:21000 #producao
#impala_shell=brgpalnxsld002.gpa.sl:21000 #dev

dt_today=$(date +'%Y-%m-%d')
dt=("2019-04-03 2019-04-15")
i=0

path_refined=/projetos/plfinanceiro/bin/Jobs/cargasEventuais/carga_full_siac_store_hist/cupons/
script_daily=${path_refined}CARGA_FULL_SIAC_STORE_HIST.EXT_CUPONS.hql
log=/projetos/plfinanceiro/log/plfinanceiro_hql.log

echo -e "\n############### NOVA EXECUCAO HQL VIA IMPALA ###############" >> $log
echo -e "\nINICIO DA CARGA FULL siac_store_hist.ext_cupons $(date +'%Y-%m-%d %H:%M:%S')" >> $log


while [ ${i} -eq 0 ]; do
 	echo -e "\nRealizando carga do dia:" $dt >> $log
 	tam=$(echo ${dt[i]} | wc -c)
 	echo $tam
 	if [ $tam -eq 11 ]; then
 		dt1=${dt[i]}
 		dt2=${dt[i]}
		impala-shell -k -i $impala_shell -f $script_daily --var=dt_today=${dt_today} --var=dt1=${dt1} --var=dt2=${dt2}  2>> $log
 	else
 		dt1=${dt[i]}
 		dt1=${dt1:0:11}
 		dt2=${dt[i]}
 		dt2=${dt2:11:22}
 		echo -e "\n############### DROPANDO PARTICOES ###############" >> $log
 		impala-shell -k -i $impala_shell -q "ALTER TABLE siac_store_hist.ext_cupons DROP IF EXISTS PARTITION (datamovto>='2019-04-03',datamovto<='2019-04-15')"
 		echo -e "\n############### APAGANDO ARQUIVOS ###############" >> $log
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-03/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-04/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-05/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-06/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-07/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-08/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-09/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-10/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-11/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-12/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-13/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-14/*
 		hdfs dfs -rm -skipTrash /gpa/rawzone/stg/siac_store_hist/ext_cupons/datamovto=2019-04-15/*
 		echo -e "\n############### INICIANDO INGESTAO DO GAP DE DATAS ###############" >> $log
 		impala-shell -k -i $impala_shell -f $script_daily --var=dt_today=${dt_today} --var=dt1=${dt1} --var=dt2=${dt2}  2>> $log
 	fi
	impala_status=$?

	if [ ${impala_status} -eq 0 ];
	then
	    echo -e "\nCarga realizada com sucesso" >> $log
	    echo -e "\nSTATUS DA CARGA ${impala_status}" >> $log
	else
	     echo -e "\nCarga realizada com erros" >> $log
	     echo -e "\nSTATUS DA CARGA ${impala_status}" >> $log
	    exit 1
	fi	
	i=$[i + 1]
done

echo -e "INVALIDATE IMPALA" >> $log
impala-shell -k -i $impala_shell -B -q "INVALIDATE METADATA siac_store_hist.ext_cupons;" >> $log

impala_status=$?

if [ ${impala_status} -eq 0 ];
then
    echo -e "FIM DA CARGA FULL siac_store_hist.ext_cupons $(date +'%Y-%m-%d %H:%M:%S')" >> $log
else
     echo -e "\nErro de invalidate no impala" >> $log
     echo -e "\nSTATUS DA CARGA ${impala_status}" >> $log
     exit 1
fi