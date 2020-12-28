SET hive.exec.max.dynamic.partitions=9000;
SET hive.exec.max.dynamic.partitions.pernode=9000;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=16;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;

-- Carga diaria da refined estrutura mercadologica

DROP TABLE IF EXISTS wrkd_financeiro.wrk_produto; 
CREATE TABLE IF NOT EXISTS wrkd_financeiro.wrk_produto AS 
SELECT 
 t1.cod_plu
,t1.nom_plu 
,t1.cod_gru
,t1.cod_subgru
,t1.cod_secao
,t1.num_seq
,t1.dat_proces
,t1.ind_ativ
FROM tszd_financeiro.produto t1
INNER JOIN ( -- Este join garante que todos os produtos sejam contemplados na sua versao mais recente
 SELECT 
 Max(concat(dat_proces,cast(coalesce(num_seq,0) as String))) as chave
 ,cod_plu
 FROM tszd_financeiro.produto 
 GROUP BY 
  cod_plu
) t_aux ON 
    t1.cod_plu = t_aux.cod_plu
AND concat(t1.dat_proces,cast(coalesce(t1.num_seq,0) as String)) = t_aux.chave
;

DROP TABLE IF EXISTS wrkd_financeiro.wrk_subgrupo;
CREATE TABLE IF NOT EXISTS wrkd_financeiro.wrk_subgrupo AS
SELECT 
 t1.cod_gru
,t1.cod_subgru
,t1.cod_secao
,t1.nom_subgru
,t1.dat_proces
FROM tszd_financeiro.subgrupo t1
INNER JOIN (
 SELECT 
  Max(concat(dat_proces,cast(coalesce(num_seq,0) as String))) as chave
 ,cod_subgru
 ,cod_gru
 ,cod_secao
 FROM tszd_financeiro.subgrupo 
 GROUP BY 
  cod_subgru
 ,cod_gru
 ,cod_secao
) t_aux ON 
    t1.cod_subgru = t_aux.cod_subgru
AND t1.cod_gru    = t_aux.cod_gru
AND t1.cod_secao  = t_aux.cod_secao
AND concat(t1.dat_proces,cast(coalesce(t1.num_seq,0) as String)) = t_aux.chave
WHERE 
    CONCAT(CAST(t1.cod_gru AS STRING), CAST(t1.cod_subgru AS STRING), CAST(t1.cod_secao AS STRING)) IS NOT NULL 
GROUP BY 
 t1.cod_gru
,t1.cod_subgru
,t1.cod_secao
,t1.nom_subgru
,t1.dat_proces
;

DROP TABLE IF EXISTS wrkd_financeiro.wrk_grupo;
CREATE TABLE IF NOT EXISTS wrkd_financeiro.wrk_grupo AS 
SELECT 
 t1.cod_gru 
,t1.cod_secao 
,t1.cod_scat 
,t1.nom_gru 
,t1.dat_proces 
FROM tszd_financeiro.grupo t1
INNER JOIN (
 SELECT 
  Max(concat(dat_proces,cast(coalesce(num_seq,0) as String))) as chave
 ,cod_gru
 ,cod_secao
 FROM tszd_financeiro.grupo
 GROUP BY 
  cod_gru
 ,cod_secao
) t_aux ON 
    t1.cod_gru    = t_aux.cod_gru
AND t1.cod_secao  = t_aux.cod_secao
AND concat(t1.dat_proces,cast(coalesce(t1.num_seq,0) as String)) = t_aux.chave
WHERE 
    CONCAT(CAST(t1.cod_gru AS STRING), CAST(t1.cod_secao AS STRING)) IS NOT NULL
GROUP BY  
 t1.cod_gru 
,t1.cod_secao 
,t1.cod_scat 
,t1.nom_gru 
,t1.dat_proces 
;

DROP TABLE IF EXISTS wrkd_financeiro.wrk_subcategoria;
CREATE TABLE IF NOT EXISTS wrkd_financeiro.wrk_subcategoria AS 
SELECT 
 t1.cod_scat 
,t1.nom_scat 
,t1.cod_categ 
,t1.dat_proces
FROM tszd_financeiro.subcategoria t1
WHERE 
    t1.dat_proces in (Select Max(dat_proces) from tszd_financeiro.subcategoria)
GROUP BY 
 t1.cod_scat 
,t1.nom_scat 
,t1.cod_categ 
,t1.dat_proces 
;

DROP TABLE IF EXISTS wrkd_financeiro.wrk_categoria;
CREATE TABLE IF NOT EXISTS wrkd_financeiro.wrk_categoria AS 
SELECT 
 t1.cod_categ 
,t1.nom_categ 
,t1.cod_depto 
,t1.dat_proces
FROM tszd_financeiro.categoria t1
WHERE 
    t1.dat_proces in (select max(dat_proces) from tszd_financeiro.categoria)
GROUP BY 
 t1.cod_categ 
,t1.nom_categ 
,t1.cod_depto 
,t1.dat_proces 
;

DROP TABLE IF EXISTS wrkd_financeiro.wrk_departamento;
CREATE TABLE IF NOT EXISTS wrkd_financeiro.wrk_departamento AS 
SELECT 
 t1.cod_depto 
,t1.nom_depto 
,t1.dat_proces 
FROM tszd_financeiro.departamento t1 
INNER JOIN (
 SELECT 
  Max(concat(dat_proces,cast(coalesce(num_seq,0) as String))) as chave
 ,cod_depto
 FROM tszd_financeiro.departamento
 GROUP BY 
  cod_depto
) t_aux ON 
    t1.cod_depto  = t_aux.cod_depto 
AND concat(t1.dat_proces,cast(coalesce(t1.num_seq,0) as String)) = t_aux.chave
WHERE 
    t1.cod_depto is not null
GROUP BY 
 t1.cod_depto 
,t1.nom_depto 
,t1.dat_proces 
;

DROP TABLE IF EXISTS wrkd_financeiro.wrk_secao;
CREATE TABLE IF NOT EXISTS wrkd_financeiro.wrk_secao AS 
SELECT 
 t1.cod_secao 
,t1.cod_depto 
,t1.nom_secao 
,t1.dat_proces 
FROM tszd_financeiro.secao t1
INNER JOIN (
 SELECT 
  Max(concat(dat_proces,cast(coalesce(num_seq,0) as String))) as chave
 ,cod_secao
 FROM tszd_financeiro.secao
 GROUP BY 
  cod_secao
) t_aux ON 
    t1.cod_secao  = t_aux.cod_secao 
AND concat(t1.dat_proces,cast(coalesce(t1.num_seq,0) as String)) = t_aux.chave
WHERE 
    t1.cod_secao IS NOT NULL
GROUP BY 
 t1.cod_secao 
,t1.cod_depto 
,t1.nom_secao 
,t1.dat_proces 
;

DROP TABLE IF EXISTS wrkd_financeiro.wrk_div_sup_div;
CREATE TABLE IF NOT EXISTS wrkd_financeiro.wrk_div_sup_div AS 
SELECT 
 t1.cod_depto 
,t1.cod_divis 
,MAX(t1.dat_ini) AS dat_ini 
FROM tszd_financeiro.div_sup_div t1
GROUP BY 
 t1.cod_depto 
,t1.cod_divis
;

DROP TABLE IF EXISTS wrkd_financeiro.wrk_divisao_superior;
CREATE TABLE IF NOT EXISTS wrkd_financeiro.wrk_divisao_superior AS 
SELECT 
 t1.cod_divis 
,t1.nom_divis 
,MAX(t1.dat_ini) AS dat_ini 
FROM tszd_financeiro.divisao_superior t1
GROUP BY 
 t1.cod_divis 
,t1.nom_divis 
;

INSERT INTO TABLE rfzd_financeiro.estrutura_mercadologica PARTITION (dt_processamento) 
SELECT 
 t1.cod_plu AS cod_plu 
,t1.nom_plu AS desc_plu 
,COALESCE(t2.cod_subgru, t1.cod_subgru) AS cod_subgrupo 
,t2.nom_subgru AS desc_subgrupo 
,COALESCE(t3.cod_gru, t1.cod_gru) AS cod_grupo 
,t3.nom_gru AS desc_grupo 
,COALESCE(t4.cod_scat, t3.cod_scat) cod_subcategoria 
,t4.nom_scat desc_subcategoria 
,COALESCE(t6.cod_secao, t1.cod_secao) AS cod_secao 
,t6.nom_secao AS desc_secao 
,COALESCE(t5.cod_categ, t4.cod_categ) AS cod_categoria 
,t5.nom_categ AS desc_categoria 
,COALESCE(t7.cod_depto, t6.cod_depto) AS cod_depto 
,t7.nom_depto AS desc_depto 
,COALESCE(t9.cod_divis, t8.cod_divis) AS cod_divisao 
,t9.nom_divis AS desc_divisao 
,t1.ind_ativ AS ind_ativo 
,"${hiveconf:dt}" AS dt_processamento
FROM wrkd_financeiro.wrk_produto t1 
LEFT JOIN wrkd_financeiro.wrk_subgrupo t2 ON
    t1.cod_gru    = t2.cod_gru 
AND t1.cod_subgru = t2.cod_subgru 
AND t1.cod_secao  = t2.cod_secao 
LEFT JOIN wrkd_financeiro.wrk_grupo t3 ON
t3.cod_gru   = t2.cod_gru 
AND t3.cod_secao = t2.cod_secao 
LEFT JOIN wrkd_financeiro.wrk_subcategoria t4 ON
t4.cod_scat = t3.cod_scat
LEFT JOIN wrkd_financeiro.wrk_categoria t5 ON
t5.cod_categ = t4.cod_categ
LEFT JOIN wrkd_financeiro.wrk_secao t6 ON
t6.cod_secao = t3.cod_secao
LEFT JOIN wrkd_financeiro.wrk_departamento t7 ON
t7.cod_depto = t6.cod_depto
LEFT JOIN wrkd_financeiro.wrk_div_sup_div t8 ON
t8.cod_depto = t7.cod_depto
LEFT JOIN wrkd_financeiro.wrk_divisao_superior t9 ON
t9.cod_divis = t8.cod_divis
;

ALTER TABLE rfzd_financeiro.estrutura_mercadologica DROP PARTITION(dt_processamento<="${hiveconf:dt_3_yrs}");

DROP TABLE IF EXISTS wrkd_financeiro.wrk_produto;
DROP TABLE IF EXISTS wrkd_financeiro.wrk_subgrupo;
DROP TABLE IF EXISTS wrkd_financeiro.wrk_grupo;
DROP TABLE IF EXISTS wrkd_financeiro.wrk_subcategoria;
DROP TABLE IF EXISTS wrkd_financeiro.wrk_categoria;
DROP TABLE IF EXISTS wrkd_financeiro.wrk_departamento;
DROP TABLE IF EXISTS wrkd_financeiro.wrk_secao;
DROP TABLE IF EXISTS wrkd_financeiro.wrk_div_sup_div;
DROP TABLE IF EXISTS wrkd_financeiro.wrk_divisao_superior;