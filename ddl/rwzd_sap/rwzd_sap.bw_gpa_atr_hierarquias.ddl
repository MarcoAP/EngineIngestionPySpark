CREATE DATABASE IF NOT EXISTS rwzd_sap;
CREATE TABLE IF NOT EXISTS rwzd_sap.bw_gpa_atr_hierarquias (
nom_hier STRING COMMENT 'Nome da hierarquia - (CAHIERNAM)',
desc_obj_hier STRING COMMENT 'Descricao dos objetos da hierarquia - (CAHCABIOB)',
num_seq STRING COMMENT 'Numero Sequencial - (CASEQUENC)',
desc_tipo_reg STRING COMMENT 'Descricao do Tipo de registro - (CAHIERTRE)',
dat_ini_valid_hier STRING COMMENT 'Data inicial validade hierarquia - (CAHCABDTF)',
dat_fim_valid_hier STRING COMMENT 'Data fim validade Hierarquia - (CAHCABDTT)',
num_nivel_list_exp_ini STRING COMMENT 'Quando aberta, ate qual nivel a hierarquia ja estara expandida - (CAHCABLEV)',
num_pos_nos_relac_folh STRING COMMENT 'Posicao dos nos em relacao as folhas do SAP - (CAHCABNPO)',
dat_hor_reg STRING COMMENT 'Timestamp do registro - (CAHCABTMS)',
cod_intern_no_raiz_hier STRING COMMENT 'Codigo interno do no raiz da hierarquia - (CAHCABRID)',
cod_idioma_hier STRING COMMENT 'Codigo de idioma da hierarquia - (CAHTXTLAN)',
txt_hier_resum STRING COMMENT 'Texto Hierarquia breve - (CAHTXTBRE)',
txt_hier_med STRING COMMENT 'Texto Hierarquia media - (CAHTXTMED)',
txt_hier_compl STRING COMMENT 'Texto Hierarquia longa - (CAHTXTLON)',
cod_intern_no STRING COMMENT 'Codigo interno no - (CAHESTNID)',
desc_no STRING COMMENT 'Descricao do no - (CAHESIOBJ)',
cod_no STRING COMMENT 'Codigo do no - (CAHESNNAM)',
num_nivel_no STRING COMMENT 'Numero do Nivel do no - (CAHESTLEV)',
cod_link_hier STRING COMMENT 'Codigo link no de hierarquia - (CAHESLINK)',
cod_intern_no_super STRING COMMENT 'Codigo interno no superior - (CAHESPAID)',
cod_intern_no_filho STRING COMMENT 'Codigo interno no filho - (CAHESCHID)',
cod_intern_no_segui STRING COMMENT 'Codigo interno no seguinte - (CAHESNEID)',
dat_ini_valid_no STRING COMMENT 'Data inicio validade no - (CAHESDFRO)',
dat_fim_valid_no STRING COMMENT 'Data fim validade no - (CAHESDTO)',
ind_no_intval STRING COMMENT 'Indicador no intervalo - (CAHESFINT)',
cod_intern_no_intval STRING COMMENT 'Codigo Interno do No Para Nos de Intervalo - (CAHINTNID) (ID de hierarquia)',
cod_no_intval_ini STRING COMMENT 'Descricao da caracteristica do objeto de intervalo inicial - (CAHINTFRO)',
cod_no_intval_fim STRING COMMENT 'Descricao da caracteristica do objeto de intervalo final - (CAHINTTO)',
cod_idioma_intval STRING COMMENT 'Codigo de idioma do intervalo - (CAHTXNLAN)',
cod_no_txt STRING COMMENT 'Codigo do no usado para obter seu texto - (CAHTXNNID)',
txt_no_resum STRING COMMENT 'Texto do no resumido - (CAHTXNBRE)',
txt_no_med STRING COMMENT 'Texto do no medio - (CAHTXNMED)',
txt_no_compl STRING COMMENT 'Texto do no completo - (CAHTXNLON)',
ind_mod_atua STRING COMMENT 'Indicador do Modo de atualizacao - (RECORDMODE)',
dt_processamento STRING COMMENT 'Data do processamento'
)
PARTITIONED BY (ano_processamento STRING COMMENT 'ano da ingestao')
ROW FORMAT DELIMITED FIELDS TERMINATED BY "\;" LINES TERMINATED BY '\n'
STORED AS PARQUET 
LOCATION  "/gpa/rawzone/rwzd_sap/bw_gpa_atr_hierarquias/"
TBLPROPERTIES ("parquet.compression"="SNAPPY",'COMMENT' = 'tabela de atributos de hierarquias do sap');