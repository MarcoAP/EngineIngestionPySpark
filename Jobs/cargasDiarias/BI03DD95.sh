#!/bin/bash
#script=/home/TC014553/git/mvp/Datalake/Refined/rfzd_financeiro/estrutura_operacional_sap.hql
#script_expurgo=/home/TC014553/git/mvp/Datalake/Refined/rfzd_financeiro/rfzd_financeiro.estrut_oper_fhpgpa_expurgo.hql
#log=/home/TC014553/git/log/plfinanceiro_hql.log
#impala_shell_ip_hml=10.183.36.168:21000
dt_process=$(date -d '-2 day' '+%Y-%m-%d')
script=/projetos/plfinanceiro/bin/Datalake/Refined/rfzd_financeiro/rfzd_financeiro.estrut_oper_fhpgpa.hql
script_expurgo=/projetos/plfinanceiro/bin/Datalake/Refined/rfzd_financeiro/rfzd_financeiro.estrut_oper_fhpgpa_expurgo.hql
log=/projetos/plfinanceiro/log/plfinanceiro_hql.log

#Producao
beeline_str="jdbc:hive2://10.150.167.101:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA;"
#Desenvolvimento
#beeline_str="jdbc:hive2://brgpalnxsld002.gpa.sl:10000/default;principal=hive/brgpalnxsld002.gpa.sl@HADOOP.DEV.GPA;"

#Producao
impala_shell_ip=brgpalnxslp006.gpa.sl:21000
#Desenvolvimento
#impala_shell_ip=brgpalnxslp006.gpa.sl:21000

echo -e "\n############### NOVA EXECUCAO HQL ###############"
echo -e "\n############### NOVA EXECUCAO HQL ###############" >> ${log}

echo -e "\nINICIO DA CARGA RFZD_FINANCEIRO.ESTRUT_OPER_FHPGPA $(date +'%Y-%m-%d %H:%M:%S')"
echo -e "\nINICIO DA CARGA RFZD_FINANCEIRO.ESTRUT_OPER_FHPGPA $(date +'%Y-%m-%d %H:%M:%S')" >> ${log}

echo -e "\nEXECUTANDO COMANDO: beeline -u ${beeline_str} --silent=true -f $script 2>> ${log}"
echo -e "\nEXECUTANDO COMANDO: beeline -u ${beeline_str} --silent=true -f $script 2>> ${log}" >> ${log}

#hive -f ${script} >> ${log}
beeline -u "${beeline_str}" --silent=true -f $script 2>> ${log}
hive_status=$?

echo -e "COMANDO beeline EXECUTADO"
echo -e "COMANDO beeline EXECUTADO" >> ${log}


echo -e "\nSTATUS DA CARGA ${hive_status}"
echo -e "\nSTATUS DA CARGA ${hive_status}" >> ${log}

echo -e "\nEXECUTANDO COMANDO: impala-shell -i ${impala_shell_ip} -q 'INVALIDATE METADATA rfzd_financeiro.estrut_oper_fhpgpa;' 2>> ${log}"
echo -e "\nEXECUTANDO COMANDO: impala-shell -i ${impala_shell_ip} -q 'INVALIDATE METADATA rfzd_financeiro.estrut_oper_fhpgpa;' 2>> ${log}" >> ${log}

impala-shell -k -i ${impala_shell_ip} -q "INVALIDATE METADATA rfzd_financeiro.estrut_oper_fhpgpa;" 2>> ${log}

echo -e 'COMANDO impala-shell EXECUTADO'
echo -e 'COMANDO impala-shell EXECUTADO' >> ${log}

echo -e "FIM DA CARGA DIARIA RFZD_FINANCEIRO.ESTRUT_OPER_FHPGPA $(date +'%Y-%m-%d %H:%M:%S')"
echo -e "FIM DA CARGA DIARIA RFZD_FINANCEIRO.ESTRUT_OPER_FHPGPA $(date +'%Y-%m-%d %H:%M:%S')" >> ${log}

echo -e "\n############### FIM DA EXECUCAO HQL ###############"
echo -e "\n############### FIM DA EXECUCAO HQL ###############" >> ${log}
#abaixo logica para data de expurgo e manter ultima estrutura do mes anterior
if [ $(date +'%d') -eq 2 ] && [ ${hive_status} -eq 0 ];
then
	echo -e "\n ULTIMA ESTRUTURA OPERACIONAL FHPGPA DO MES ANTERIOR MANTIDA"
else
	beeline -u "${beeline_str}" -hiveconf dt_process=${dt_process} -f ${script_expurgo} >> ${log}
fi
