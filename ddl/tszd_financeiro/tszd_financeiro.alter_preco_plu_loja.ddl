CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.alter_preco_plu_loja (
 cod_loja					INT			COMMENT 'Codigo da loja'
,cod_plu					INT			COMMENT 'Codigo PLU'
,cod_mot_alter				SMALLINT	COMMENT 'Codigo Motivo da Alteracao'
,desc_mot_alter				STRING		COMMENT 'Descricao Motivo da Alteracao'
,cod_usu_alter_preco		STRING		COMMENT 'Codigo Usuario Alteracao do Preco'
,val_ant					DOUBLE		COMMENT 'Valor Anterior a alteracao de preco por RD'
,val_vig					DOUBLE		COMMENT 'Valor Vigente (pos alteracao de preco por RD)'
,dat_alter_val				STRING		COMMENT 'Data de Alteracao do Valor'
,hor_alter_val				STRING		COMMENT 'Hora da Alteracao do Valor'
,cod_senha					STRING		COMMENT 'Senha criptografa disponibilizada para a loja que permite a  alteracao de preco do produto.'
,qtd_dia_valid				SMALLINT	COMMENT 'Quantidade dias Validade'
,cod_secao					INT			COMMENT 'Codigo da Secao do PLU'
,cod_fami_plu				INT			COMMENT 'Codigo da Familia do PLU'
,ind_valid_senha_fami		BOOLEAN		COMMENT 'Codigo de Validacao de senha para Familia (false=Senha invalida para Familia / true=Senha valida para familia)'
,dt_processamento			STRING		COMMENT 'Data de processamento da ingestao'
) STORED AS PARQUET
TBLPROPERTIES('parquet.compression'='SNAPPY', 'comment' = 'tabela de senhas utilizadas, origem vb(mx2)');