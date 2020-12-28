SET hive.exec.dynamic.partition = true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
SET hive.optimize.ppd=true;
SET hive.optimize.ppd.storage=true;
SET hive.stats.autogather=true;
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;

DROP TABLE IF EXISTS wrkd_financeiro.estrut_oper_fhpgpa_wrk;

CREATE TABLE wrkd_financeiro.estrut_oper_fhpgpa_wrk AS 
SELECT 
 t1.nom_hier
,t1.desc_obj_hier
,t1.num_seq
,t1.desc_tipo_reg
,t1.dat_ini_valid_hier
,t1.dat_fim_valid_hier
,t1.num_nivel_list_exp_ini
,t1.num_pos_nos_relac_folh
,t1.dat_hor_reg
,t1.cod_intern_no_raiz_hier
,t1.cod_idioma_hier
,t1.txt_hier_resum
,t1.txt_hier_med
,t1.txt_hier_compl
,t1.cod_intern_no
,t1.desc_no
,t1.cod_no
,t1.num_nivel_no
,t1.cod_link_hier
,t1.cod_intern_no_super
,t1.cod_intern_no_filho
,t1.cod_intern_no_segui
,t1.dat_ini_valid_no
,t1.dat_fim_valid_no
,t1.ind_no_intval
,t1.cod_intern_no_intval
,t1.cod_no_intval_ini
,t1.cod_no_intval_fim
,t1.cod_idioma_intval
,t1.cod_no_txt
,t1.txt_no_resum
,t1.txt_no_med
,t1.txt_no_compl
,t1.ind_mod_atua
,t1.dt_processamento
FROM rwzd_sap.bw_gpa_atr_hierarquias t1
WHERE t1.dt_processamento = "${hiveconf:dt_process}"
and t1.desc_obj_hier="0COSTCENTER"
;

INSERT INTO TABLE rfzd_financeiro.estrut_oper_fhpgpa PARTITION(ano_processamento)
SELECT
 SUBSTR(associa.caract_1, 5, LENGTH(associa.caract_1)) as cod_estrut_geral
,t1_desc.txt_no_compl as desc_estrut_geral
,SUBSTR(associa.caract_2, 5, LENGTH(associa.caract_1)) as cod_empr_gru
,t2_desc.txt_no_compl as desc_empr_gru
,SUBSTR(associa.caract_3, 5, LENGTH(associa.caract_1)) as cod_empr_sgru
,t3_desc.txt_no_compl as desc_empr_sgru
,SUBSTR(associa.caract_4, 5, LENGTH(associa.caract_1)) as cod_bu
,t4_desc.txt_no_compl as desc_bu
,SUBSTR(associa.caract_5, 5, LENGTH(associa.caract_1)) as cod_band
,t5_desc.txt_no_compl as desc_band
,SUBSTR(associa.caract_6, 5, LENGTH(associa.caract_1)) as cod_regiao
,t6_desc.txt_no_compl as desc_regiao
,SUBSTR(associa.caract_7, 5, LENGTH(associa.caract_1)) as cod_region
,t7_desc.txt_no_compl as desc_region
,CASE WHEN CAST(SUBSTR(associa.caract_8, 5, LENGTH(associa.caract_1)) AS INT) IS NULL THEN SUBSTR(associa.caract_8, 5, LENGTH(associa.caract_1)) ELSE CAST(CAST(SUBSTR(associa.caract_8, 5, LENGTH(associa.caract_1)) AS INT) AS STRING) END as cod_loja -- Existem codigos de loja que sao apenas inteiros, e outros que possuem caracteres. Esse case garante que os inteiros nao tenham 0 a esquerda
,t8_desc.txt_centro_custo_med as desc_loja
,"${hiveconf:dt_process}" as dt_processamento
,CAST(SUBSTR("${hiveconf:dt_process}",1,4) AS INT) AS ano_processamento
FROM ( -- Esta query retorna a estrutura do SAP ja montada, sem os txts (descricoes)
SELECT 
 CAST(t8.cod_intern_no as INT) as id_interno_8
,TRIM(t8.cod_no) as caract_8
,CAST(t7.cod_intern_no as INT) as id_interno_7
,TRIM(t7.cod_no) as caract_7
,CAST(t6.cod_intern_no as INT) as id_interno_6
,TRIM(t6.cod_no) as caract_6
,CAST(t5.cod_intern_no as INT) as id_interno_5
,TRIM(t5.cod_no) as caract_5
,CAST(t4.cod_intern_no as INT) as id_interno_4
,TRIM(t4.cod_no) as caract_4
,CAST(t3.cod_intern_no as INT) as id_interno_3
,TRIM(t3.cod_no) as caract_3
,CAST(t2.cod_intern_no as INT) as id_interno_2
,TRIM(t2.cod_no) as caract_2
,CAST(t1.cod_intern_no as INT) as id_interno_1
,TRIM(t1.cod_no) as caract_1
FROM wrkd_financeiro.estrut_oper_fhpgpa_wrk t8 
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t7 
ON (CAST(t8.cod_intern_no_super AS INT) = CAST(t7.cod_intern_no AS INT)
AND CAST(t7.num_nivel_no AS INT) = 7 -- Nivel do no
AND TRIM(t7.desc_tipo_reg) = "ESTRNODE" -- Tipo de registro usado para montar a estrutura/cascata
AND UPPER(TRIM(t7.nom_hier)) = "ACPAFHPGPA") -- Nome da estrutura que iremos montar
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t6 
ON (CAST(t7.cod_intern_no_super AS INT) = CAST(t6.cod_intern_no AS INT)
AND CAST(t6.num_nivel_no AS INT) = 6
AND TRIM(t6.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t6.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t5 
ON (CAST(t6.cod_intern_no_super AS INT) = CAST(t5.cod_intern_no AS INT)
AND CAST(t5.num_nivel_no AS INT) = 5
AND TRIM(t5.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t5.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t4 
ON (CAST(t5.cod_intern_no_super AS INT) = CAST(t4.cod_intern_no AS INT)
AND CAST(t4.num_nivel_no AS INT) = 4
AND TRIM(t4.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t4.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t3 
ON (CAST(t4.cod_intern_no_super AS INT) = CAST(t3.cod_intern_no AS INT)
AND CAST(t3.num_nivel_no AS INT) = 3
AND TRIM(t3.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t3.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t2 
ON (CAST(t3.cod_intern_no_super AS INT) = CAST(t2.cod_intern_no AS INT)
AND CAST(t2.num_nivel_no AS INT) = 2
AND TRIM(t2.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t2.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t1 
ON (CAST(t2.cod_intern_no_super AS INT) = CAST(t1.cod_intern_no AS INT)
AND CAST(t1.num_nivel_no AS INT) = 1
AND TRIM(t1.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t1.nom_hier)) = "ACPAFHPGPA")
WHERE CAST(t8.num_nivel_no AS INT) = 8
AND TRIM(t8.desc_tipo_reg)= "ESTRNODE"
AND UPPER(TRIM(t8.nom_hier)) = "ACPAFHPGPA" 
) associa
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t1_desc -- A partir daqui, vamos juntar cada nivel da estrutura com suas devidas descricoes
ON (associa.caract_1 = TRIM(t1_desc.cod_no_txt)
AND UPPER(TRIM(t1_desc.desc_tipo_reg)) = "TXTNODE" -- Tipo de registro usado para buscar os txts (descricoes)
AND UPPER(TRIM(t1_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t2_desc
ON (associa.caract_2 = TRIM(t2_desc.cod_no_txt)
AND UPPER(TRIM(t2_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t2_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t3_desc
ON (associa.caract_3 = TRIM(t3_desc.cod_no_txt)
AND UPPER(TRIM(t3_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t3_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t4_desc
ON (associa.caract_4 = TRIM(t4_desc.cod_no_txt)
AND UPPER(TRIM(t4_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t4_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t5_desc
ON (associa.caract_5 = TRIM(t5_desc.cod_no_txt)
AND UPPER(TRIM(t5_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t5_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t6_desc
ON (associa.caract_6 = TRIM(t6_desc.cod_no_txt)
AND UPPER(TRIM(t6_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t6_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t7_desc
ON (associa.caract_7 = TRIM(t7_desc.cod_no_txt)
AND UPPER(TRIM(t7_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t7_desc.nom_hier)) = "ACPAFHPGPA")
LEFT JOIN (SELECT * FROM rwzd_sap.bw_gpa_txt_ccusto t2_aux WHERE t2_aux.dt_processamento = "${hiveconf:dt_process}") t8_desc -- Tabela usada para buscar os txts (descricoes) do nivel mais baixo da estrutura
ON (SUBSTR(associa.caract_8,1,4) = TRIM(t8_desc.desc_area_contab_custo)
AND TRIM(SUBSTR(associa.caract_8,5,LENGTH(associa.caract_8))) = TRIM(t8_desc.cod_centro_custo)
AND UPPER(TRIM(t8_desc.cod_idioma)) = "P"
AND TRIM(t8_desc.dat_valid_fim) = "99991231")
WHERE 
    TRIM(t8_desc.cod_centro_custo) IS NOT NULL -- Garante que apenas os registros que estao na tabela de TXTs serao considerados
GROUP BY 
 associa.id_interno_1
,associa.caract_1
,t1_desc.txt_no_compl
,associa.id_interno_2
,associa.caract_2
,t2_desc.txt_no_compl
,associa.id_interno_3
,associa.caract_3
,t3_desc.txt_no_compl
,associa.id_interno_4
,associa.caract_4
,t4_desc.txt_no_compl
,associa.id_interno_5
,associa.caract_5
,t5_desc.txt_no_compl
,associa.id_interno_6
,associa.caract_6
,t6_desc.txt_no_compl
,associa.id_interno_7
,associa.caract_7
,t7_desc.txt_no_compl
,associa.id_interno_8
,associa.caract_8
,t8_desc.txt_centro_custo_med

UNION ALL -- Esta query e muito similar a outra, mas trata apenas do RANGE de IDs que precisam ser 'explodidos'

SELECT
 SUBSTR(associa.caract_1, 5, LENGTH(associa.caract_1)) as cod_estrut_geral
,t1_desc.txt_no_compl as desc_estrut_geral
,SUBSTR(associa.caract_2, 5, LENGTH(associa.caract_1)) as cod_empr_gru
,t2_desc.txt_no_compl as desc_empr_gru
,SUBSTR(associa.caract_3, 5, LENGTH(associa.caract_1)) as cod_empr_sgru
,t3_desc.txt_no_compl as desc_empr_sgru
,SUBSTR(associa.caract_4, 5, LENGTH(associa.caract_1)) as cod_bu
,t4_desc.txt_no_compl as desc_bu
,SUBSTR(associa.caract_5, 5, LENGTH(associa.caract_1)) as cod_band
,t5_desc.txt_no_compl as desc_band
,SUBSTR(associa.caract_6, 5, LENGTH(associa.caract_1)) as cod_regiao
,t6_desc.txt_no_compl as desc_regiao
,SUBSTR(associa.caract_7, 5, LENGTH(associa.caract_1)) as cod_region
,t7_desc.txt_no_compl as desc_region
,CASE WHEN CAST(associa.caract_8 AS INT) IS NULL THEN CAST(associa.caract_8 AS STRING) ELSE CAST(CAST(associa.caract_8 AS INT) AS STRING) END as cod_loja
,t8_desc.txt_centro_custo_med as desc_loja
,"${hiveconf:dt_process}" as dt_processamento
,CAST(SUBSTR(CAST(CURRENT_DATE() AS STRING),1,4) AS INT) AS ano_processamento
FROM ( -- E a mesma tabela que associa a estrutura, mas nao traz os TXTs (descricoes)
SELECT 
 TRIM(CAST(t8.cod_no AS STRING)) as caract_8
,CAST(t7.cod_intern_no as INT) as id_interno_7
,TRIM(t7.cod_no) as caract_7
,CAST(t6.cod_intern_no as INT) as id_interno_6
,TRIM(t6.cod_no) as caract_6
,CAST(t5.cod_intern_no as INT) as id_interno_5
,TRIM(t5.cod_no) as caract_5
,CAST(t4.cod_intern_no as INT) as id_interno_4
,TRIM(t4.cod_no) as caract_4
,CAST(t3.cod_intern_no as INT) as id_interno_3
,TRIM(t3.cod_no) as caract_3
,CAST(t2.cod_intern_no as INT) as id_interno_2
,TRIM(t2.cod_no) as caract_2
,CAST(t1.cod_intern_no as INT) as id_interno_1
,TRIM(t1.cod_no) as caract_1
FROM ( -- Segundo passo da 'explosao', une a t_range com a LATERAL VIEW
 SELECT
  t_range.inicio + aux.i as cod_no -- Aqui o ID inicial e somado a coluna 'i', criando todos os IDs necessarios
 ,t_range.inicio as id_interno_inicio 
 ,t_range.fim as id_interno_fim 
 ,t_range.id_interno_pai as cod_intern_no_super 
 ,t_range.id_interno_tmp as id_interno_tmp 
 ,aux.i 
 FROM ( -- Aqui e o primeiro passo da 'explosao'
  SELECT 
   CAST(SUBSTR(t2_intvl.cod_no_intval_ini,5,LENGTH(t2_intvl.cod_no_intval_ini)) AS INT) as inicio -- Remove o prefixo dos codigos
  ,CAST(SUBSTR(t2_intvl.cod_no_intval_fim,5,LENGTH(t2_intvl.cod_no_intval_fim)) AS INT) as fim -- Remove o prefixo dos codigos
  ,CAST(t2_intvl.cod_intern_no_intval AS INT) as id_interno_tmp
  ,CAST(t1_intvl.cod_intern_no_super AS INT) as id_interno_pai
  FROM wrkd_financeiro.estrut_oper_fhpgpa_wrk t1_intvl
  INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t2_intvl -- Une ela com ela mesma, cruzando o id interno do SAP com a coluna que possui esse mesmo id interno para casos de RANGE
  ON (CAST(t1_intvl.cod_intern_no AS INT) = CAST(t2_intvl.cod_intern_no_intval AS INT)
  AND UPPER(TRIM(t2_intvl.desc_tipo_reg)) = "INTVLNODE") -- Tipo de registro usado para buscar os intervalos
  WHERE 
      UPPER(TRIM(t1_intvl.nom_hier)) = "ACPAFHPGPA"
  AND UPPER(TRIM(t1_intvl.ind_no_intval)) = "X" -- Indica que e um no de intervalo
  AND UPPER(TRIM(t1_intvl.desc_tipo_reg)) = "ESTRNODE" -- Tipo de registro usado para montar a estrutura/cascata
 ) t_range
 -- SPACE gera uma coluna com o numero de espacos especificado
 -- SPLIT por ' ' cria um array splitando por espacos, ou seja, ele vai criar um array com o mesmo numero de espacos gerado pelo SPACE
 -- POSEXPLODE vai explodir esse array, na variavel s teremos o conteudo, enquanto na variavel i, teremos o numero dele no array (comecando por 1)
 LATERAL VIEW posexplode(split(space(t_range.fim - t_range.inicio),' ')) aux as i,s
) t8 -- A partir daqui, e tudo igual a query anterior
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t7 
ON (CAST(t8.cod_intern_no_super AS INT) = CAST(t7.cod_intern_no AS INT)
AND CAST(t7.num_nivel_no AS INT) = 7
AND TRIM(t7.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t7.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t6 
ON (CAST(t7.cod_intern_no_super AS INT) = CAST(t6.cod_intern_no AS INT)
AND CAST(t6.num_nivel_no AS INT) = 6
AND TRIM(t6.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t6.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t5 
ON (CAST(t6.cod_intern_no_super AS INT) = CAST(t5.cod_intern_no AS INT)
AND CAST(t5.num_nivel_no AS INT) = 5
AND TRIM(t5.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t5.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t4 
ON (CAST(t5.cod_intern_no_super AS INT) = CAST(t4.cod_intern_no AS INT)
AND CAST(t4.num_nivel_no AS INT) = 4
AND TRIM(t4.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t4.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t3 
ON (CAST(t4.cod_intern_no_super AS INT) = CAST(t3.cod_intern_no AS INT)
AND CAST(t3.num_nivel_no AS INT) = 3
AND TRIM(t3.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t3.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t2 
ON (CAST(t3.cod_intern_no_super AS INT) = CAST(t2.cod_intern_no AS INT)
AND CAST(t2.num_nivel_no AS INT) = 2
AND TRIM(t2.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t2.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t1 
ON (CAST(t2.cod_intern_no_super AS INT) = CAST(t1.cod_intern_no AS INT)
AND CAST(t1.num_nivel_no AS INT) = 1
AND TRIM(t1.desc_tipo_reg) = "ESTRNODE"
AND UPPER(TRIM(t1.nom_hier)) = "ACPAFHPGPA")
) associa
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t1_desc
ON (associa.caract_1 = TRIM(t1_desc.cod_no_txt)
AND UPPER(TRIM(t1_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t1_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t2_desc
ON (associa.caract_2 = TRIM(t2_desc.cod_no_txt)
AND UPPER(TRIM(t2_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t2_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t3_desc
ON (associa.caract_3 = TRIM(t3_desc.cod_no_txt)
AND UPPER(TRIM(t3_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t3_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t4_desc
ON (associa.caract_4 = TRIM(t4_desc.cod_no_txt)
AND UPPER(TRIM(t4_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t4_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t5_desc
ON (associa.caract_5 = TRIM(t5_desc.cod_no_txt)
AND UPPER(TRIM(t5_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t5_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t6_desc
ON (associa.caract_6 = TRIM(t6_desc.cod_no_txt)
AND UPPER(TRIM(t6_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t6_desc.nom_hier)) = "ACPAFHPGPA")
INNER JOIN wrkd_financeiro.estrut_oper_fhpgpa_wrk t7_desc
ON (associa.caract_7 = TRIM(t7_desc.cod_no_txt)
AND UPPER(TRIM(t7_desc.desc_tipo_reg)) = "TXTNODE"
AND UPPER(TRIM(t7_desc.nom_hier)) = "ACPAFHPGPA")
LEFT JOIN (SELECT * FROM rwzd_sap.bw_gpa_txt_ccusto t2_aux WHERE t2_aux.dt_processamento = "${hiveconf:dt_process}") t8_desc
ON (TRIM(associa.caract_8) = TRIM(t8_desc.cod_centro_custo)
AND UPPER(TRIM(t8_desc.desc_area_contab_custo)) = "ACPA"
AND UPPER(TRIM(t8_desc.cod_idioma)) = "P"
AND TRIM(t8_desc.dat_valid_fim) = "99991231")
WHERE
    TRIM(t8_desc.cod_centro_custo) IS NOT NULL
GROUP BY 
 associa.id_interno_1
,associa.caract_1
,t1_desc.txt_no_compl
,associa.id_interno_2
,associa.caract_2
,t2_desc.txt_no_compl
,associa.id_interno_3
,associa.caract_3
,t3_desc.txt_no_compl
,associa.id_interno_4
,associa.caract_4
,t4_desc.txt_no_compl
,associa.id_interno_5
,associa.caract_5
,t5_desc.txt_no_compl
,associa.id_interno_6
,associa.caract_6
,t6_desc.txt_no_compl
,associa.id_interno_7
,associa.caract_7
,t7_desc.txt_no_compl
,associa.caract_8
,associa.caract_8
,t8_desc.txt_centro_custo_med
;


DROP TABLE IF EXISTS wrkd_financeiro.estrut_oper_fhpgpa_wrk;
