#!/bin/bash
dt_ini=("2017-01-01" "2017-02-01" "2017-03-01" "2017-04-01" "2017-05-01" "2017-06-01" "2017-07-01" "2017-08-01" "2017-09-01" "2017-10-01" "2017-11-01" "2017-12-01" "2018-01-01" "2018-02-01" "2018-03-01" "2018-04-01" "2018-05-01" "2018-06-01" "2018-07-01" "2018-08-01" "2018-09-01" "2018-10-01" "2018-11-01" "2018-12-01" "2019-01-01" "2019-02-01" "2019-03-01" "2019-04-01" "2019-05-01" "2019-06-01")
dt_fim=("2017-01-31" "2017-02-28" "2017-03-31" "2017-04-30" "2017-05-31" "2017-06-30" "2017-07-31" "2017-08-31" "2017-09-30" "2017-10-31" "2017-11-30" "2017-12-31" "2018-01-31" "2018-02-28" "2018-03-31" "2018-04-30" "2018-05-31" "2018-06-30" "2018-07-31" "2018-08-31" "2018-09-30" "2018-10-31" "2018-11-30" "2018-12-31" "2019-01-31" "2019-02-28" "2019-03-31" "2019-04-30" "2019-05-31" "2019-06-30")
impala_shell_producao=brgpalnxslp006.gpa.sl
#impala_shell_homologacao=10.183.36.168
#dev
#hive="jdbc:hive2://brgpalnxsld002.gpa.sl:10000/default;principal=hive/brgpalnxsld002.gpa.sl@HADOOP.DEV.GPA;"
#prod
hive="jdbc:hive2://brgpalnxslp005.gpa.sl:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA;"
dois_anos_em_meses=29
i=0
echo -e "########## Dropando dados da Estrutura Mercadologica Refined  ##########"
beeline -u ${hive} --silent=false  -e "alter table rfzd_financeiro.estrutura_mercadologica drop partition (dt_processamento<='2019-06-30')"
while [ ${dois_anos_em_meses} -ge ${i} ]; do
	# A partir de janeiro de 2017
	echo -e "${dt_ini[$i]} ${dt_fim[$i]}"
	beeline -u $hive -hiveconf dt_ini=${dt_ini[$i]}  -hiveconf dt_fim=${dt_fim[$i]} -f /projetos/plfinanceiro/bin/Datalake/Refined/rfzd_financeiro/BI03DD46_full.hql
	#hive -hiveconf dt_ini=${dt_ini[$i]} -hiveconf dt_fim=${dt_fim[$i]} -f /projetos/plfinanceiro/bin/Datalake/Refined/rfzd_financeiro/BI03DD46_full.hql
	#impala-shell --var=dt_ini=${dt_ini[$i]} --var=dt_fim=${dt_fim[$i]} -i ${impala_shell_homologacao}:21000 -B -f refined_v2.hql 
	#impala-shell --var=dt_ini=${dt_ini[$i]} --var=dt_fim=${dt_fim[$i]} -i ${impala_shell_producao}:21000 -B -f refined_v2.hql 
	i=$(expr $i + 1)
done
echo -e "Fim da carga da rfzd_financeiro.estrutura_mercadologica"