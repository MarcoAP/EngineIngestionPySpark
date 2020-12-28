CREATE DATABASE IF NOT EXISTS rwzd_amelia;
CREATE TABLE IF NOT EXISTS rwzd_amelia.end_entrega (
 cod_cli			String Comment 'Codigo do Cliente'
,num_seq_end		String Comment 'Numero sequencia do Endereco'
,cod_cep			String Comment 'Codigo CEP'
,cod_tipo_lograd	String Comment 'Codigo Tipo Logradouro'
,nom_lograd			String Comment 'Nome Logradouro'
,num_lograd			String Comment 'Numero Logradouro'
,desc_compl_lograd	String Comment 'Descricao Complemento do Logradouro'
,nom_bairro			String Comment 'Nome do Bairro'
,nom_munic			String Comment 'Nome do Municipio'
,sgl_uf				String Comment 'Sigla UF'
,desc_ponto_refer	String Comment 'Descricao Ponto de Referencia'
,desc_end			String Comment 'Descricao do Endereco'
,ind_corresp		String Comment 'Indicador Correspondencia'
,dt_processamento	String Comment 'Data de Processamento'
)	STORED AS PARQUET
	LOCATION '/gpa/rawzone/rwzd_amelia/end_entrega/'
	TBLPROPERTIES ('parquet.compression'='SNAPPY', 'comment' = 'Arquivo com relacao do endereco de entrega com cliente. Orgiem: Amelia')
;