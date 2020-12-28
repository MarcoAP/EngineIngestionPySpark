CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.det_solic_senha (
 dat_ini_valid_senha	STRING	COMMENT	'Data de inicio da validade da senha'
,dat_envio_email_senha	STRING	COMMENT	'Data de envio do email de senha'
,cod_usu_cad_senha		STRING	COMMENT	'Codigo do usuario que cadastrou a senha'
,nom_usu_cad_senha		STRING	COMMENT	'Nome do usuario que cadastrou a senha'
,desc_mot_lib_senha		STRING	COMMENT	'Descricao do motivo de liberacao da senha'
,cod_loja				INT	    COMMENT	'Codigo da loja'
,nom_loja				STRING	COMMENT	'Nome da loja'
,val_plu				DOUBLE	COMMENT	'Valor do produto'
,cod_depto				INT	    COMMENT	'Codigo do departamento'
,nom_depto				STRING	COMMENT	'Nome do departamento'
,cod_categ				INT	    COMMENT	'Codigo da categoria'
,nom_categ				STRING	COMMENT	'Nome da categoria'
,cod_scat				INT	    COMMENT	'Código da subcategoria'
,nom_scat				STRING	COMMENT	'Nome da subcategoria'
,cod_secao				INT	    COMMENT	'Codigo secao'
,nom_secao				STRING	COMMENT	'Nome seção'
,cod_plu				INT	    COMMENT	'codigo plu'
,nom_plu				STRING	COMMENT	'Nome do produto plu'
,cod_senha				STRING	COMMENT	'Senha criptografa disponibilizada para a loja que permite a  alteração de preço do produto.'
,dt_processamento        STRING  COMMENT 'Data do processamento'
) STORED AS PARQUET
TBLPROPERTIES('parquet.compression'='SNAPPY','auto.purge'='true', 'comment' = 'tabela de senhas solicitadas, origem ac34');