CREATE DATABASE IF NOT EXISTS rfzd_financeiro;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.apur_cmv_ge01 (
 cod_loja     				  INT 	 COMMENT 'Codigo da loja'
,cod_plu       				  INT    COMMENT 'Codigo do produto'
,dat_movto    				  INT    COMMENT 'Data'
,val_unit_custo_estq_final    DOUBLE COMMENT 'Custo sobre Mercadoria Vendida'
,dt_processamento 			  STRING COMMENT 'Data de processamento') 
   STORED AS PARQUET 
   LOCATION '/gpa/refinedzone/rfzd_financeiro/apur_cmv_ge01' 
   TBLPROPERTIES ('parquet.compression'='SNAPPY','comment'='tabela de cmv dos produtos por loja e dia retirado de 30d retroativos do GE01')
;