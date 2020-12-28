#!/bin/bash
impala_shell_producao=10.150.167.96
impala_shell_homologacao=10.183.36.168

script=/projetos/plfinanceiro/bin/Datalake/Refined/rfzd_financeiro/reproces_senhas_nao_utilizadas.hql
log=/home/TC014665/projetos/plfinanceiro/log/plfinanceiro.log

dt_proces=$1
echo -e "\nINICIO DO REPROCESSAMENTO RFZD_FINANCEIRO.SENHA_NAO_UTILIZADA DATA: $1" >> $log

hive -hiveconf dt_proces=${dt_proces} -S -f $script 2>> $log
hive_status=$?

echo -e "\nSTATUS DO REPROCESSAMENTO ${hive_status}" >> $log

impala-shell -i impala_shell_producao:21000 -B -q "INVALIDATE METADATA rfzd_financeiro.senha_nao_utilizada;" 2>> $log

echo -e "FIM DO REPROCESSAMENTO RFZD_FINANCEIRO.SENHA_NAO_UTILIZADA $1 " >> $log