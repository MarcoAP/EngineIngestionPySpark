CREATE DATABASE IF NOT EXISTS rfzv_financeiro;
CREATE VIEW IF NOT EXISTS rfzv_financeiro.senha_diaria AS 
SELECT 
	 x.cod_senha
	,x.cod_usu_cad_senha
	,x.nom_usu_cad_senha
	,x.cod_usu_alter_preco
	,x.nom_usu_alter_preco
	,x.dat_envio_email_senha
	,x.dat_alter_val
	,x.hor_alter_val
	,x.qtd_itens_venda
	,x.val_ant
	,x.val_vig
	,x.val_descon_plu
	,x.val_aplic_rd
	,x.val_vend_bruta
	,x.ind_valid_senha_fami
	,x.desc_mot_lib_senha
	,coalesce(x.ind_senha_cria, false) ind_senha_cria
	,x.ind_senha_local
	,x.dat_valid_senha
	,x.cod_plu
	,x.nom_plu
	,x.cod_subgru
	,x.nom_subgru
	,x.cod_gru
	,x.nom_gru
	,x.cod_scat
	,x.nom_scat
	,x.cod_secao
	,x.nom_secao
	,x.cod_categ
	,x.nom_categ
	,x.cod_depto
	,x.nom_depto
	,x.cod_divis
	,x.nom_divis
	,x.cod_loja
	,x.nom_loja
	,x.sgl_uf_loja
	,x.cod_micro_reg
	,x.nom_micro_reg
	,x.cod_reg
	,x.nom_reg
	,x.cod_band
	,x.nom_band
	,x.cod_bu
	,x.nom_bu
	,x.cod_multivarejo
	,x.nom_multivarejo
	,x.ind_senha_local
	,x.dt_processamento 
FROM (
	SELECT
			 z.cod_senha
			,z.cod_usu_cad_senha
			,z.nom_usu_cad_senha
			,z.cod_usu_alter_preco
			,z.nom_usu_alter_preco
			,z.dat_envio_email_senha
			,z.dat_alter_val
			,z.hor_alter_val
			,z.qtd_itens_venda
			,z.val_ant
			,z.val_vig
			,z.val_descon_plu
			,z.val_aplic_rd
			,z.val_vend_bruta
			,z.ind_valid_senha_fami
			,z.desc_mot_lib_senha
			,z.ind_senha_cria
			,z.ind_senha_local
			,z.dat_valid_senha
			,z.cod_plu
			,z.nom_plu
			,z.cod_subgru
			,z.nom_subgru
			,z.cod_gru
			,z.nom_gru
			,z.cod_scat
			,z.nom_scat
			,z.cod_secao
			,z.nom_secao
			,z.cod_categ
			,z.nom_categ
			,z.cod_depto
			,z.nom_depto
			,z.cod_divis
			,z.nom_divis
			,z.cod_loja
			,z.nom_loja
			,z.sgl_uf_loja
			,z.cod_micro_reg
			,z.nom_micro_reg
			,z.cod_reg
			,z.nom_reg
			,z.cod_band
			,z.nom_band
			,z.cod_bu
			,z.nom_bu
			,z.cod_multivarejo
			,z.nom_multivarejo
			,z.dt_processamento
	FROM (
		SELECT 
			 CAST(trim(senha_utilizada.cod_senha) AS STRING) cod_senha
			,CAST(trim(senha_utilizada.cod_usu_cad_senha) AS STRING) cod_usu_cad_senha
			,CAST(trim(senha_utilizada.nom_usu_cad_senha) AS STRING) nom_usu_cad_senha
			,CAST(trim(senha_utilizada.cod_usu_alter_preco) AS STRING) cod_usu_alter_preco
			,CAST(trim(senha_utilizada.nom_usu_alter_preco) AS STRING) nom_usu_alter_preco
			,CAST(trim(senha_utilizada.dat_envio_email_senha) AS STRING) dat_envio_email_senha
			,CAST(trim(senha_utilizada.dat_alter_val) AS STRING) dat_alter_val
			,CAST(trim(senha_utilizada.hor_alter_val) AS STRING) hor_alter_val
			,senha_utilizada.qtd_itens_venda
			,senha_utilizada.val_ant
			,senha_utilizada.val_vig
			,CAST(trim(senha_utilizada.val_descon_plu) AS STRING) val_descon_plu
			,senha_utilizada.val_aplic_rd
			,senha_utilizada.val_vend_bruta
			,senha_utilizada.ind_valid_senha_fami
			,CAST(trim(senha_utilizada.desc_mot_lib_senha) AS STRING) desc_mot_lib_senha
			,senha_utilizada.ind_senha_cria
			,senha_utilizada.ind_senha_local
			,NULL dat_valid_senha
			,senha_utilizada.cod_plu
			,CAST(trim(senha_utilizada.nom_plu) AS STRING) nom_plu
			,senha_utilizada.cod_subgru
			,CAST(trim(senha_utilizada.nom_subgru) AS STRING) nom_subgru
			,senha_utilizada.cod_gru
			,CAST(trim(senha_utilizada.nom_gru) AS STRING) nom_gru
			,senha_utilizada.cod_scat
			,CAST(trim(senha_utilizada.nom_scat) AS STRING) nom_scat
			,senha_utilizada.cod_secao
			,CAST(trim(senha_utilizada.nom_secao) AS STRING) nom_secao
			,senha_utilizada.cod_categ
			,CAST(trim(senha_utilizada.nom_categ) AS STRING) nom_categ
			,senha_utilizada.cod_depto
			,CAST(trim(senha_utilizada.nom_depto) AS STRING) nom_depto
			,senha_utilizada.cod_divis
			,CAST(trim(senha_utilizada.nom_divis) AS STRING) nom_divis
			,senha_utilizada.cod_loja
			,CAST(trim(senha_utilizada.nom_loja) AS STRING) nom_loja
			,CAST(trim(senha_utilizada.sgl_uf_loja) AS STRING) sgl_uf_loja
			,senha_utilizada.cod_micro_reg
			,CAST(trim(senha_utilizada.nom_micro_reg) AS STRING) nom_micro_reg
			,senha_utilizada.cod_reg
			,CAST(trim(senha_utilizada.nom_reg) AS STRING) nom_reg
			,senha_utilizada.cod_band
			,CAST(trim(senha_utilizada.nom_band) AS STRING) nom_band
			,senha_utilizada.cod_bu
			,CAST(trim(senha_utilizada.nom_bu) AS STRING) nom_bu
			,senha_utilizada.cod_multivarejo
			,CAST(trim(senha_utilizada.nom_multivarejo) AS STRING) nom_multivarejo
			,CAST(trim(senha_utilizada.dt_processamento) AS STRING) dt_processamento
		FROM rfzd_financeiro.senha_utilizada 
	) z
	
	UNION ALL 

	SELECT 
		 y.cod_senha
		,y.cod_usu_cad_senha
		,y.nom_usu_cad_senha
		,y.cod_usu_alter_preco
		,y.nom_usu_alter_preco
		,y.dat_envio_email_senha
		,y.dat_alter_val
		,y.hor_alter_val
		,y.qtd_itens_venda
		,y.val_ant
		,y.val_vig
		,y.val_descon_plu
		,y.val_aplic_rd
		,y.val_vend_bruta
		,y.ind_valid_senha_fami
		,y.desc_mot_lib_senha
		,y.ind_senha_cria
		,y.ind_senha_local
		,y.dat_valid_senha
		,y.cod_plu
		,y.nom_plu
		,y.cod_subgru
		,y.nom_subgru
		,y.cod_gru
		,y.nom_gru
		,y.cod_scat
		,y.nom_scat
		,y.cod_secao
		,y.nom_secao
		,y.cod_categ
		,y.nom_categ
		,y.cod_depto
		,y.nom_depto
		,y.cod_divis
		,y.nom_divis
		,y.cod_loja
		,y.nom_loja
		,y.sgl_uf_loja
		,y.cod_micro_reg
		,y.nom_micro_reg
		,y.cod_reg
		,y.nom_reg
		,y.cod_band
		,y.nom_band
		,y.cod_bu
		,y.nom_bu
		,y.cod_multivarejo
		,y.nom_multivarejo
		,y.dt_processamento
	FROM (
		SELECT 
			 CAST(trim(senha_nao_utilizada.cod_senha) AS STRING) cod_senha
			,CAST(trim(senha_nao_utilizada.cod_usu_solic_senha) AS STRING) cod_usu_cad_senha
			,CAST(trim(senha_nao_utilizada.nom_usu_solic_senha) AS STRING) nom_usu_cad_senha
			,NULL cod_usu_alter_preco
			,NULL nom_usu_alter_preco
			,CAST(trim(senha_nao_utilizada.dat_envio_email_senha) AS STRING) dat_envio_email_senha
			,NULL dat_alter_val
			,NULL hor_alter_val
			,NULL qtd_itens_venda
			,NULL val_ant
			,NULL val_vig
			,NULL val_descon_plu
			,NULL val_aplic_rd
			,NULL val_vend_bruta
			,NULL ind_valid_senha_fami
			,CAST(trim(senha_nao_utilizada.desc_mot_lib_senha) AS STRING) desc_mot_lib_senha
			,NULL ind_senha_cria
            ,NULL ind_senha_local
			,CAST(trim(senha_nao_utilizada.dat_valid_senha) AS STRING) dat_valid_senha
			,senha_nao_utilizada.cod_plu
			,CAST(trim(senha_nao_utilizada.nom_plu) AS STRING) nom_plu
			,senha_nao_utilizada.cod_subgru
			,CAST(trim(senha_nao_utilizada.nom_subgru) AS STRING) nom_subgru
			,senha_nao_utilizada.cod_gru
			,CAST(trim(senha_nao_utilizada.nom_gru) AS STRING) nom_gru
			,senha_nao_utilizada.cod_scat
			,CAST(trim(senha_nao_utilizada.nom_scat) AS STRING) nom_scat
			,senha_nao_utilizada.cod_secao
			,CAST(trim(senha_nao_utilizada.nom_secao) AS STRING) nom_secao
			,senha_nao_utilizada.cod_categ
			,CAST(trim(senha_nao_utilizada.nom_categ) AS STRING) nom_categ
			,senha_nao_utilizada.cod_depto
			,CAST(trim(senha_nao_utilizada.nom_depto) AS STRING) nom_depto
			,senha_nao_utilizada.cod_divis
			,CAST(trim(senha_nao_utilizada.nom_divis) AS STRING) nom_divis
			,senha_nao_utilizada.cod_loja
			,CAST(trim(senha_nao_utilizada.nom_loja) AS STRING) nom_loja
			,CAST(trim(senha_nao_utilizada.sgl_uf_loja) AS STRING) sgl_uf_loja
			,senha_nao_utilizada.cod_micro_reg
			,CAST(trim(senha_nao_utilizada.nom_micro_reg) AS STRING) nom_micro_reg
			,senha_nao_utilizada.cod_reg
			,NULL nom_reg
			,NULL cod_band
			,NULL nom_band
			,senha_nao_utilizada.cod_bu
			,CAST(trim(senha_nao_utilizada.nom_bu) AS STRING) nom_bu
			,senha_nao_utilizada.cod_multivarejo
			,CAST(trim(senha_nao_utilizada.nom_multivarejo) AS STRING) nom_multivarejo
			,MAX(CAST(trim(senha_nao_utilizada.dt_processamento) AS STRING)) dt_processamento
		FROM rfzd_financeiro.senha_nao_utilizada
		
	    GROUP BY
			cod_senha
			,cod_usu_solic_senha
			,nom_usu_solic_senha
			,dat_envio_email_senha
			,desc_mot_lib_senha
			,dat_valid_senha
			,cod_plu
			,nom_plu
			,cod_subgru
			,nom_subgru
			,cod_gru
			,nom_gru
			,cod_scat
			,nom_scat
			,cod_secao
			,nom_secao
			,cod_categ
			,nom_categ
			,cod_depto
			,nom_depto
			,cod_divis
			,nom_divis
			,cod_loja
			,nom_loja
			,sgl_uf_loja
			,cod_micro_reg
			,nom_micro_reg
			,cod_reg
			,nom_reg
			,cod_band
			,nom_band
			,cod_bu
			,nom_bu
			,cod_multivarejo
			,nom_multivarejo
	) y
) x;