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

CREATE TABLE wrkd_financeiro.estrut_oper_fhpgpa_tmp_expurgo AS 
SELECT 
 cod_estrut_geral
,desc_estrut_geral
,cod_empr_gru
,desc_empr_gru
,cod_empr_sgru
,desc_empr_sgru
,cod_bu
,desc_bu
,cod_band
,desc_band
,cod_regiao
,desc_regiao
,cod_region
,desc_region
,cod_loja
,desc_loja
,dt_processamento
,ano_processamento
FROM rfzd_financeiro.estrut_oper_fhpgpa
WHERE 
    dt_processamento <> "${hiveconf:dt_process}"
;

TRUNCATE TABLE rfzd_financeiro.estrut_oper_fhpgpa;

INSERT INTO TABLE rfzd_financeiro.estrut_oper_fhpgpa PARTITION(ano_processamento)
SELECT 
 cod_estrut_geral
,desc_estrut_geral
,cod_empr_gru
,desc_empr_gru
,cod_empr_sgru
,desc_empr_sgru
,cod_bu
,desc_bu
,cod_band
,desc_band
,cod_regiao
,desc_regiao
,cod_region
,desc_region
,cod_loja
,desc_loja
,dt_processamento
,ano_processamento
FROM wrkd_financeiro.estrut_oper_fhpgpa_tmp_expurgo
;

DROP TABLE wrkd_financeiro.estrut_oper_fhpgpa_tmp_expurgo PURGE;
