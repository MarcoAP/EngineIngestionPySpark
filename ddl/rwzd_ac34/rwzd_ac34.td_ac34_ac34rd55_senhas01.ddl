CREATE DATABASE IF NOT EXISTS rwzd_ac34;
CREATE TABLE IF NOT EXISTS rwzd_ac34.td_ac34_ac34rd55_senhas01 (   
  data_validade STRING COMMENT 'Data de validade'
  ,data_envio_email STRING COMMENT 'Data de envio do email'
  ,cod_user STRING COMMENT 'Codigo do usuario'
  ,nome_usuario STRING COMMENT 'Nome do usuario'
  ,desc_motivo STRING COMMENT 'Descricao do motivo'
  ,cod_ucbd STRING COMMENT 'Codigo UCBD'
  ,nome_cplt_ucbd STRING COMMENT 'Nome CPLT UCBD'
  ,val_preco_definido_usuario STRING COMMENT 'Valor do preco definido pelo usuario'
  ,num_dpto STRING COMMENT 'Numero departamento'
  ,nome_dpto STRING COMMENT 'Nome departamento'
  ,num_unng STRING COMMENT 'Numero UNNG'
  ,nome_unng STRING COMMENT 'Nome UNNG'
  ,num_sung STRING COMMENT 'Numero SUNG'
  ,nome_sung STRING COMMENT 'Nome SUNG'
  ,num_seca STRING COMMENT 'Numero SECA'
  ,nome_seca STRING COMMENT 'Nome SECA'
  ,idt_plu STRING COMMENT 'IDT PLU'
  ,desc_cmpl_prod STRING COMMENT 'Descricao complementar do produto'
  ,desc_senha STRING COMMENT 'Descricao da senha' 
  ,dt_processamento     STRING COMMENT 'Data de Processamento') 
PARTITIONED BY (mes_processamento STRING COMMENT 'Mes de processamento')  
STORED AS PARQUET 
LOCATION '/gpa/rawzone/rwzd_ac34/td_ac34_ac34rd55_senhas01' 
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT' = 'Tabela de senhas da Raw do AC34')