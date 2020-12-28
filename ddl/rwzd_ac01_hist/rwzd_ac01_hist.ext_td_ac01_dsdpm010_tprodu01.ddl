	CREATE DATABASE IF NOT EXISTS rwzd_ac01_hist;
	CREATE EXTERNAL TABLE IF NOT EXISTS rwzd_ac01_hist.ext_td_ac01_dsdpm010_tprodu01 (
	  ind_acao string COMMENT 'Indicador de Acao I- INSERT, D- DELETE, U- UPDATE', 
	  cod_reduzido string COMMENT 'Codigo de barra interno do GPA (plu)', 
	  nom_prod string  COMMENT 'Nome completo do produto', 
	  nom_prod_resum string COMMENT 'Descricao resumida do produto', 
	  cod_familia string COMMENT 'Codigo da familia de produtos', 
	  cod_fabric_merc string  COMMENT	'Codigo do fornecedor', 
	  cod_secao string COMMENT	'Codigo da Secao', 
	  cod_grupo string COMMENT 'Codigo do grupo', 
	  cod_subgrupo string COMMENT 'Codigo do subgrupo', 
	  flg_marca_propria string COMMENT 'Flag Produto Marca Propria "S" – Produto de Marca Propria "H" - Produto de Marca Propria Hibrido " " – Produto nao e de Marca Propria', 
	  ind_tip_marca_propria string COMMENT 'Indicador do tipo da marca propria Formato:   "I" – Produto de Marca Propria Industrializado "P" – Produto de Marca Propria de Fabricacao Propria " " – Produto nao e de Marca Propria  ' , 
	  ind_peso_variavel string COMMENT 'Indicador de peso variavel 	Formato:   "S" – Produto de Peso Variavel "N" – Produto nao e de Peso Variavel', 
	  tip_uso_prod string COMMENT 'Tipo do uso do produto. Podem ser: REVENDA MATERIAL DE ESCRITORIO MATERIAL DE LIMPEZA UNIFORMES EMBALAGEM DE ITENS DE REVENDA MATERIA PRIMA BRINDE....', 
	  dat_inclusao string COMMENT 'Data da Inclusao do Produto Formato: AAAAMMDD', 
	  cod_qualif string COMMENT 'Codigo da marca do produto marca propria', 
      ind_prod_regional string COMMENT 'Indicador de produto regional 	Formato:   "S" – Produto  Regional " " – Produto nao  Regional ', 
      ind_prod_pp string COMMENT 'Tipo de tributacao do produto' , 
	  tip_tribut string  , 
	  ind_escolha_economica string COMMENT 'Indicador de escolha economica', 
 	  cod_sub_grupo_soluc string COMMENT 'Codigo do subgrupo solucao',
 	  cod_grupo_soluc string COMMENT 'Codigo do grupo solucao',  
	  ind_bem_estar string COMMENT 'Indicador de produto bem estar' , 
	  ind_organico string COMMENT 'Indicador de produto organico', 
 	  cod_ncm string COMMENT 'Codigo da NCM ou NBM, dependendo do tipo (Nomenclatura Comum Mercosul)', 
 	  cod_ppb_portaria string COMMENT 'Codigo da portaria do Governo para beneficio fiscal PPB - processo produtivo basico', 
	  cod_ppb_ano_portaria string COMMENT 'Ano da portaria do Governo para beneficio fiscal PPB - processo produtivo basico', 
	  ind_cod_reduz_digito string COMMENT 'Indicador de digito verificador de PLU Indicador.Pode ser: (S/N)', 
 	  uni_alt_planog string COMMENT 'Altura do produto', 
 	  uni_larg_planog string COMMENT 'Largura do produto', 
	  uni_comp_planog string  COMMENT 'Altura do produto', 
	  sigl_tipo_classific_prod string COMMENT 'Tipo de venda ao qual o subtipo esta vinculado.', 
 	  sigl_subtipo_classific_prod string  COMMENT 'Subtipo de venda.', 
 	  cod_especific_classific_prod string COMMENT 'Codigo da especificacao da garantia.', 
	  ind_foto_prod string  COMMENT 'Indica se o produto possui foto.', 
	  val_preco_min_prod string COMMENT 'Valor do preco minimo do produto', 
 	  val_preco_max_prod string COMMENT 'Valor do preco maximo do produto', 
 	  ind_tipo_associacao_prod string  COMMENT 'Indicador para os tipos de associacoes entre produtos (PLU).', 
	  cod_reduzido_ponte string COMMENT 'Codigo reduzido para o PLU Ponte. Obrigatorio para os PLUs de custo sem divergencia (amarracao nacional OK).', 
	  num_sequencial string COMMENT 'Numero Sequencial do Registro dentro do Arquivo'
	  ) PARTITIONED BY (dt_processamento STRING COMMENT 'data da ingestao' )
	ROW FORMAT 
	DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/stg/rwzd_ac01_hist/ext_td_ac01_dsdpm010_tprodu01'
	TBLPROPERTIES ('parquet.compression'='SNAPPY','COMMENT'= 'Tabela de produtos extracao de novos registros, registros que sofreram alteracao e delecao da
	 view diaria SC de referencia SC-TAB-EAN-Y2K')
	;