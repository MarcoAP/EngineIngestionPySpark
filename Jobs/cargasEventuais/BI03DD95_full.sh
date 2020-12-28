#!/bin/bash
dt_process=("2019-04-30" "2019-05-31" "2019-06-06")
script=/projetos/plfinanceiro/bin/Datalake/Refined/rfzd_financeiro/rfzd_financeiro.estrut_oper_fhpgpa_full.hql
log=/projetos/plfinanceiro/log/plfinanceiro_hql.log

#Producao
beeline_str="jdbc:hive2://10.150.167.101:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA;"
#Desenvolvimento
#beeline_str="jdbc:hive2://brgpalnxsld002.gpa.sl:10000/default;principal=hive/brgpalnxsld002.gpa.sl@HADOOP.DEV.GPA;"

#Producao
impala_shell_ip=brgpalnxslp006.gpa.sl:21000
#Desenvolvimento
#impala_shell_ip=brgpalnxslp006.gpa.sl:21000
i=0


beeline -u "${beeline_str}" --silent=true -e "TRUNCATE TABLE rfzd_financeiro.estrut_oper_fhpgpa;"


while [ ${i} -lt 3 ]; do

	echo -e "\n############### NOVA EXECUCAO HQL ###############"
	echo -e "\n############### NOVA EXECUCAO HQL ###############" >> ${log}

	echo -e "\nINICIO DA CARGA RFZD_FINANCEIRO.ESTRUT_OPER_FHPGPA ${dt_process[i]}"
	echo -e "\nINICIO DA CARGA RFZD_FINANCEIRO.ESTRUT_OPER_FHPGPA ${dt_process[i]}" >> ${log}

	echo -e "\nEXECUTANDO COMANDO: beeline -u ${beeline_str} --silent=true -f $script 2>> ${log}"
	echo -e "\nEXECUTANDO COMANDO: beeline -u ${beeline_str} --silent=true -f $script 2>> ${log}" >> ${log}

	#hive -f ${script} >> ${log}
	beeline -u "${beeline_str}" --silent=true -hiveconf dt_process=${dt_process[$i]} -f $script 2>> ${log}
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

	echo -e "FIM DA CARGA DIARIA RFZD_FINANCEIRO.ESTRUT_OPER_FHPGPA ${dt_process[i]}"
	echo -e "FIM DA CARGA DIARIA RFZD_FINANCEIRO.ESTRUT_OPER_FHPGPA ${dt_process[i]}" >> ${log}

	echo -e "\n############### FIM DA EXECUCAO HQL ###############"
	echo -e "\n############### FIM DA EXECUCAO HQL ###############" >> ${log}

	i=$[i + 1]
done