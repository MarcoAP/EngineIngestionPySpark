CREATE DATABASE IF NOT EXISTS rfzd_financeiro;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.senha_nao_utilizada
(
 cod_senha     STRING COMMENT "Senha criptografa disponibilizada para a loja que permite a  alteracao de precoo do produto."
,cod_usu_solic_senha  STRING  COMMENT "Codigo de loja do usuario que emitiu a senha no ac34" 
,nom_usu_solic_senha  STRING  COMMENT "Nome do usuario que emitiu a senha no ac34"
,dat_envio_email_senha STRING COMMENT 'Data de envio do email de senha'
,desc_mot_lib_senha   STRING  COMMENT "Descricao do motivo de criacao da senha" 
,dat_valid_senha  STRING COMMENT 'Data de validade da senha'
,cod_plu     INT  COMMENT "Codigo de barra interno do GPA (plu)" 
,nom_plu    STRING COMMENT "Nome do produto plu" 
,cod_subgru    INT  COMMENT "Codigo do subgrupo" 
,nom_subgru    STRING  COMMENT "Nome do subgrupo" 
,cod_gru      INT  COMMENT "Codigo do grupo" 
,nom_gru     STRING  COMMENT "Nome do grupo" 
,cod_scat     INT  COMMENT "Codigo do subcategoria" 
,nom_scat     STRING  COMMENT "Nome da subcategoria" 
,cod_secao     INT  COMMENT "Codigo da secao" 
,nom_secao     STRING  COMMENT "Nome da secao" 
,cod_categ     INT  COMMENT "Codigo da categoria" 
,nom_categ     STRING  COMMENT "Nome da categoria" 
,cod_depto     INT  COMMENT "Codigo do departamento" 
,nom_depto     STRING  COMMENT "Nome do departamento" 
,cod_divis     INT  COMMENT "Codigo da divisao" 
,nom_divis     STRING  COMMENT "Nome da divisao" 
,cod_loja     INT  COMMENT "Codigo da loja" 
,nom_loja     STRING  COMMENT "Nome da loja. Estrutura 7.1"
,sgl_uf_loja    CHAR(2) COMMENT "Unidade federativa da loja"
,cod_micro_reg    INT  COMMENT "Codigo da Microregiao da loja. Estrutura 7.1"
,nom_micro_reg    STRING  COMMENT "Nome da Microregiao da loja. Estrutura 7.1"
,cod_reg     INT  COMMENT "Codigo da Regiao da loja. Estrutura 7.1"
,nom_reg        STRING  COMMENT "Nome da Regiao da loja. Estrutura 7.1"
,cod_band     INT  COMMENT "Codigo da Bandeira da loja. Estrutura 7.1"
,nom_band     STRING  COMMENT "Nome Bandeira da loja. Estrutura 7.1"
,cod_bu     INT  COMMENT "Codigo da BU da loja. Estrutura 7.1"
,nom_bu     STRING  COMMENT "Nome da BU da loja. Estrutura 7.1"
,cod_multivarejo   INT  COMMENT "Codigo para Utilizado para agregar todas as lojas e suas cascatas. Estrutura 7.1"
,nom_multivarejo   STRING  COMMENT "Nome para Utilizado para agregar todas as lojas e suas cascatas. Estrutura 7.1"
,dt_processamento             STRING  COMMENT 'Data do processamento'
) 
STORED AS PARQUET 
TBLPROPERTIES ("parquet.compression"="SNAPPY", "COMMENT" = "Tabela com visao geral das senhas nao utilizadas")
;