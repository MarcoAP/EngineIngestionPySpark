CREATE DATABASE IF NOT EXISTS rwzd_amelia;
CREATE TABLE IF NOT EXISTS rwzd_amelia.pedido (
 num_ped				String Comment 'Numero do Pedido'
,cod_band				String Comment 'Codigo da Bandeira'
,cod_atend				String Comment 'Codigo do Atendente'
,cod_cli				String Comment 'Codigo do Cliente'
,num_seq_end			String Comment 'Numero Sequencia Endereco'
,cod_loja				String Comment 'Codigo da Loja'
,cod_status				String Comment 'Codigo Status'
,dat_cria				String Comment 'Data da Criacao do Pedido'
,dat_entreg_prev		String Comment 'Data Entrega Prevista'
,dat_entreg_efetua		String Comment 'Data Entrega Efetuada'
,val_taxa_entreg		String Comment 'Valor Taxa de Entrega'
,cod_orig				String Comment 'Codigo Origem'
,cod_tipo_entreg		String Comment 'Codigo Tipo de Entrega'
,cod_ped_site			String Comment 'Codigo Pedido Site'
,val_bonus				String Comment 'Valor Bonus'
,num_seq_cobr			String Comment 'Numero Sequencia Cobranca'
,val_ini_ped			String Comment 'Valor Inicio Pedido'
,ind_arq				String Comment 'Indicador do Arquivo'
,ind_tipo_nf			String Comment 'Indicador da Nota Fiscal'
,num_seq_end_fat		String Comment 'Numero Sequencia Endereco Fatura'
,nom_cli_entreg			String Comment 'Nome Cliente Entrega'
,num_cpf_cli_entreg		String Comment 'Numero Cpf Cliente Entrega'
,nom_dest_msg			String Comment 'Nome Destino Mensagem'
,nom_orig_msg			String Comment 'Nome Origem Mensagem'
,cod_instit				String Comment 'Codigo da Instituicao'
,ind_cel				String Comment 'Indicador Celular'
,ind_copa_mastercard	String Comment 'Indicador Copa Mastercard'
,ind_gol_extra			String Comment 'Indicador Goleada Extra'
,cod_sit_cred			String Comment 'Codigo Situacao Credito'
,ind_presen				String Comment 'Indicador Presente'
,ind_ped_pv				String Comment 'Indicador Pedido PV'
,num_cupom_fiscal		String Comment 'Numero do Cupom Fiscal'
,cod_pdv				String Comment 'Codigo do PDV'
,ind_venda_express		String Comment 'Indicador Venda Express'
,dat_venda				String Comment 'Data da Venda'
,dt_processamento		String Comment 'Data de Processamento'
)	PARTITIONED BY (ano_cria STRING Comment 'Ano da Criacao do Pedido')
	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_amelia/pedido/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com dados de pedidos. Orgiem: Amelia')
;