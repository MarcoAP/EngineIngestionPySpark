CREATE DATABASE IF NOT EXISTS rfzd_financeiro;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.apur_metr_ecommerce (
dat_venda 					STRING Comment 'Data da venda'
,cod_loja					int    Comment 'Codigo da Loja'
,cod_plu					int    Comment 'Codigo Produto'
,val_tot_vend_bruta_plu		double Comment 'Valor Total Venda Bruta'
,val_tot_vend_liq_plu		double Comment 'Valor Total Venda Liquida'
,val_cmv					double Comment 'Valor CMV - Custo da Mercadoria Vendida'
,val_margem_pdv				double Comment 'Valor Margem PDV'
,ind_tipo_vend_exp			String Comment 'Indicador Tipo Venda EX - CD - LJ'
,desc_plu 					STRING COMMENT "Nome completo do produto"
,desc_loja					STRING COMMENT "Descricao Loja"
,cod_subgrupo 				INT COMMENT "Codigo do subgrupo"
,desc_subgrupo 				STRING COMMENT "Descricao do subgrupo"
,cod_grupo 					INT COMMENT "Codigo do grupo"
,desc_grupo 				STRING COMMENT "Descricao do grupo"
,cod_subcategoria 			INT COMMENT "Codigo do subcategoria"
,desc_subcategoria 			STRING COMMENT "Descricao da subcategoria"
,cod_secao 					INT COMMENT "Codigo da secao"
,desc_secao 				STRING COMMENT "Descricao da secao"
,cod_categoria 				INT COMMENT "Codigo da categoria"
,desc_categoria 			STRING COMMENT "Descricao da categoria"
,cod_departamento 			INT COMMENT "Codigo do departamento"
,desc_departamento 			STRING COMMENT "Descricao do departamento"
,cod_divisao 				INT COMMENT "Codigo da divisao"
,desc_divisao 				STRING COMMENT "Descricao da divisao"
,cod_estrut_geral			STRING COMMENT "Codigo Estrutura Geral"
,desc_estrut_geral			STRING COMMENT "Descricao Estrutura Geral"
,cod_empr_gru				STRING COMMENT "Codigo Empresas Grupo"
,desc_empr_gru				STRING COMMENT "Descricao Empresas Grupo"
,cod_empr_sgru				STRING COMMENT "Codigo Empresas Subgrupo"
,desc_empr_sgru				STRING COMMENT "Descricao Empresas Subgrupo"
,cod_bu						STRING COMMENT "Codigo BU"
,desc_bu					STRING COMMENT "Descricao BU"
,cod_band					STRING COMMENT "Codigo Bandeira"
,desc_band					STRING COMMENT "Descricao Bandeira"
,cod_regiao					STRING COMMENT "Codigo Regiao"
,desc_regiao				STRING COMMENT "Descricao Regiao"
,cod_region					STRING COMMENT "Codigo Regional"
,desc_region				STRING COMMENT "Descricao Regional"
)	PARTITIONED BY (dt_processamento STRING )
	STORED AS PARQUET
	LOCATION '/gpa/refinedzone/rfzd_financeiro/apur_metr_ecommerce/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Tabela Processada com Apuracao de Metricas do Ecommerce e Multivarejo.')
;