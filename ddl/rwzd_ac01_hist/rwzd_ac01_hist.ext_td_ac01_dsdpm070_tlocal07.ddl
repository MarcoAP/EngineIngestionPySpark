CREATE DATABASE IF NOT EXISTS rwzd_ac01_hist;
CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac01_hist.ext_td_ac01_dsdpm070_tlocal07 (
	  ind_acao string, 
	  ind_sit string, 
	  cod_emp string, 
	  cod_uf_loja string, 
	  cod_loja string, 
	  cod_tipo_local string, 
  	  cod_conc string, 
  	  dat_sit_local string, 
	  dat_inaug_local string, 
	  nom_loja string, 
	  cod_bandeira string, 
	  cod_gru_varejo string, 
	  dat_atu_reg string, 
	  tam_area_loja string, 
	  cod_reg_venda string, 
	  cod_loja_mae string, 
	  tip_loja_filha string, 
	  qtd_check_out string, 
	  qtd_balanca string, 
	  hora_ini_receb string, 
	  hora_fin_receb string, 
 	  ind_loja_propria string, 
 	  ind_loja_24horas string, 
 	  ind_tipo_loja string, 
	  cod_conc_neg string, 
	  ind_tip_oper string, 
	  num_cgc_loja string, 
	  num_cgc_barra_loja string, 
	  num_dig_cgc_loja string, 
	  end_loja string, 
	  nom_bairro_loja string, 
	  nom_cid_loja string, 
	  num_ddd_loja string, 
	  num_tel_loja string, 
  	  nom_gerente string, 
	  nom_regional string, 
	  hor_abertura string, 
	  hor_fechamento string, 
	  tam_area_venda string, 
	  num_sequencial string 
	) PARTITIONED BY (dt_processamento STRING  COMMENT 'data da ingestao')
	ROW FORMAT 
	DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm070_tlocal07'
	TBLPROPERTIES ('parquet.compression'='SNAPPY')
	;