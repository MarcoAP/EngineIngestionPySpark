CREATE TABLE rfzd_financeiro.apur_bonif (
dat_bonif STRING NOT NULL COMMENT "data da bonificacao" 
,cod_loja INT NOT NULL COMMENT "Codigo da Loja" 
,cod_plu INT NOT NULL COMMENT "Código do produto"
,cod_secao INT NOT NULL COMMENT "Codigo da secao"
,cod_gru INT NOT NULL COMMENT 'Codigo do grupo'
,cod_tipo_bonif INT NOT NULL COMMENT 'Codigo do tipo de bonificacao'
,cod_fornec INT NOT NULL COMMENT "Código do fornecedor"
,cod_cta INT NOT NULL COMMENT "Código da conta"
,val_bonif DOUBLE NOT NULL COMMENT "Valor da binificacao"
,PRIMARY KEY (dat_bonif , cod_loja , cod_plu , cod_secao, cod_gru, cod_tipo_bonif, cod_fornec, cod_cta)
) 
PARTITION BY HASH (dat_bonif) PARTITIONS 4
STORED AS KUDU
--TBLPROPERTIES ('kudu.master_addresses'='brgpalnxsld002.gpa.sl') --dev
TBLPROPERTIES ('kudu.master_addresses'='brgpalnxslp003.gpa.sl,brgpalnxslp004.gpa.sl,brgpalnxslp005.gpa.sl') --procucao 
;


