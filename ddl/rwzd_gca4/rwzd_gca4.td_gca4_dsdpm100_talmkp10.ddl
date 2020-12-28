CREATE TABLE IF NOT EXISTS rwzd_gca4.td_gca4_dsdpm100_talmkp10 (
 uid_indicador     STRING COMMENT "Indicador de operação (inserir/atualizar/deletar)"
,ano_mes_movto     STRING COMMENT "Ano e mes do movimento"
,cod_loja 		   STRING COMMENT "Código da loja"
,cod_plu 		   STRING COMMENT "Código do produto"
,dia_ocorrencia    STRING COMMENT "Dia da ocorrencia"
,cod_tip_bonif     STRING COMMENT "Código do tipo de bonificação"
,cod_reg_compra    STRING COMMENT "Código da região da compra"
,cod_fornec 	   STRING COMMENT "Código do fornecedor"
,cod_dep 		   STRING COMMENT "Codigo do centro de distribuicao (deposito)"
,cod_ccusto 	   STRING COMMENT "Código do centro de custo"
,val_alocacao 	   STRING COMMENT "Valor da alocacao"
,val_custo_sv_base STRING COMMENT "Valor base de custo sobre venda"
,num_sequencial	   STRING COMMENT "Sequencia da ordem de processamento" )
PARTITIONED BY (dt_processamento STRING COMMENT "Data de processamento" )
STORED AS PARQUET
LOCATION '/gpa/rawzone/rwzd_gca4/td_gca4_dsdpm100_talmkp10'
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT'='Tabela de Markup')  
;