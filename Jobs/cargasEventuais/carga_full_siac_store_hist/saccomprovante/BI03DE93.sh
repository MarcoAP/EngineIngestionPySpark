#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
impala_shell=brgpalnxslp006.gpa.sl:21000 #producao
#impala_shell=brgpalnxsld002.gpa.sl:21000 #dev


path_refined=/projetos/plfinanceiro/bin/Jobs/cargasEventuais/carga_full_siac_store_hist/saccomprovante/
script_daily=${path_refined}BI03DE93.hql
log=/projetos/plfinanceiro/log/plfinanceiro_hql.log

echo -e "\n############### NOVA EXECUCAO HQL VIA IMPALA ###############" >> $log
echo -e "\nINICIO DA CARGA FULL siac_store_hist.ext_saccomprovante $(date +'%Y-%m-%d %H:%M:%S')" >> $log


impala-shell -k -i $impala_shell -f $script_daily 2>> $log
impala_status=$?

echo -e "\STATUS CARGA $impala_status" 2>> $log

echo -e "INVALIDATE IMPALA" >> $log
impala-shell -k -i $impala_shell -B -q "INVALIDATE METADATA siac_store_hist.ext_saccomprovante;" >> $log

