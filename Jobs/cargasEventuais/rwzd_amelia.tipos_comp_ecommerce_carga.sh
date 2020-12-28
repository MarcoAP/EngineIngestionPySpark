#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
impala_shell=brgpalnxslp006.gpa.sl:21000 #producao
#impala_shell=brgpalnxsld002.gpa.sl:21000 #dev
#dev
hive="jdbc:hive2://brgpalnxsld002.gpa.sl:10000/default;principal=hive/brgpalnxsld002.gpa.sl@HADOOP.DEV.GPA"
#prod
hive="jdbc:hive2://10.150.167.101:10000/default;principal=hive/10.150.167.101@HADOOP.PRD.GPA"

beeline -u $hive --silent=true -q "INSERT into TABLE rwzd_amelia.tipos_comp_ecommerce(cod_tipo_compra,desc_tipo_compra,dt_processamento) \
									values ('1','Compra Perfeita','2019-04-18'), \
        							('2','Compra Completa','2019-04-18'), \
       	 							('3','Compra Incompleta','2019-04-18');" 

impala-shell -k -i $impala_shell -B -q "INVALIDATE METADATA rwzd_amelia.tipos_comp_ecommerce;"