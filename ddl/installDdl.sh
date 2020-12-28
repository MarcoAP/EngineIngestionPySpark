#!/bin/bash
########################################################
##
## Author: Marco Antonio Pereira
## Date: January 8th, 2019
## Description: Instalador de DLL
##
########################################################
set +x
########################################################
## Variaveis globais
########################################################
hostDefault=$(echo $(hostname))
hostProd="brgpacla10p.gpa.sl"		## Host de Producao
hostHomol="10.183.36.168"			## Host de Homolog
impalaConf="-i "$hostDefault" -B"	## IP Connect impala
diretorio=$1						## Dir com DLL's
opcao=$2							## Opcao de controle
table=$3							## Tabela especifica

########################################################
# opcao=d ou D - Drop and Create table if not exists
# opcao=dd ou DD - Drop with PURGE
# opcao=c ou C - Drop and Create a specific table
# opcao "c" exige especificar table em $3.ddl
# opcao=qualquer coisa - Create table if not exists
########################################################

function arquivosDLL () {
	listaArquivos=$(ls -x $diretorio | grep -v ^d )
	echo -e $listaArquivos | sed -e "s/ /\n/g"
}

function instalarDllbeeline -u jdbc:hive2://10.150.167.101:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA() {
	for item in $(arquivosDLL)
	do 	
		if [ "$opcao" != '' ];
		then
			if [ $opcao = "d" -o $opcao = "D" ];
			then
				beeline -u jdbc:hive2://10.150.167.101:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA-e "DROP TABLE IF EXISTS "$(echo $item | sed -e "s/.ddl//g")" PURGE;"
				beeline -u jdbc:hive2://10.150.167.101:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA-f $(echo $diretorio"/"$item | sed -e "s/\n//g")
			else
				if [ $opcao = "dd" -o $opcao = "DD" -o $opcao = "Dd" -o $opcao = "dD" ];
				then
					beeline -u jdbc:hive2://10.150.167.101:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA-e 'DROP TABLE IF EXISTS '$(echo $item | sed -e "s/.ddl//g")' PURGE;'
				else
					if [ [ $opcao = "c" -o $opcao = "C" ] -a [ "$table" != '' ] ];
					then
						drop="DROP TABLE IF EXISTS "$(echo $(echo $table | cut -d'/' -f 2) | cut -d'.' -f 1)"."$(echo $(echo $table | cut -d'/' -f 2) | cut -d'.' -f 2)
						beeline -u jdbc:hive2://10.150.167.101:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA-e '${drop}'
						beeline -u jdbc:hive2://10.150.167.101:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA-f $(echo $diretorio"/"$item | sed -e "s/\n//g")
					fi
				fi
			fi
		else
			beeline -u jdbc:hive2://10.150.167.101:10000/default;principal=hive/brgpalnxslp005.gpa.sl@HADOOP.PRD.GPA-f $(echo $diretorio"/"$item | sed -e "s/\n//g")
		fi
	done
}

function invalidateMetadata () {
	impala-shell $impalaConf -q "INVALIDATE METADATA;"
}

function main () {
	instalarDllHive
	invalidateMetadata
}

## Exec
main