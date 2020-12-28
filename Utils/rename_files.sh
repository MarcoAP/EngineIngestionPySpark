#!/bin/bash
########################################################
##
## Author: Marco Antonio Pereira
## Date: January 23th, 2019
## Description: Rename file names
##
########################################################
for file in $(find . | egrep ".00F")
do
	echo ">> ${file}"
  	data_certa=$(echo "20"$(echo $(echo ${file} | cut -d'_' -f 2) | cut -d'.' -f 1) | egrep -o '[[:digit:]]{8}' | head -n1)
  	arquivo_certo="rwzd_vb.ext_mx2.${data_certa}.csv"
  	echo $arquivo_certo
	mv "$file" "$arquivo_certo"
done
rm -rf rename_files.sh
ls -lash .