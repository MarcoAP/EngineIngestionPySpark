CREATE DATABASE IF NOT EXISTS tszd_financeiro;
CREATE TABLE IF NOT EXISTS tszd_financeiro.apur_rd (
   cod_loja          INT    COMMENT 'Codigo da loja'
  ,cod_plu           BIGINT COMMENT 'Codigo do PLU' --codproduto
  ,num_term          INT    COMMENT 'Numero do terminal da loja no qual a venda foi efetuada' --nroterminal
  ,num_cupom         INT    COMMENT 'Numero do cupom/ticket' --nrocupom
  ,cod_tpo_rd        INT    COMMENT 'Codigo do tipo da RD'
  ,cod_tpo_ofer      INT    COMMENT 'Codigo do tipo da oferta'
  ,cod_stpo_ofer     INT    COMMENT 'Codigo do subtipo da oferta'
  ,val_aplic_rd      DOUBLE COMMENT 'Valor aplicado ao produto pela RD (Remarcacao/Demarcacao)'
  ,val_liq_aplic_rd  DOUBLE COMMENT 'Valor liquido aplicado ao produto pela RD (Remarcacao/Demarcacao)'
  ,val_fator_calc_imp DOUBLE COMMENT 'Valor do fator para cálculo do imposto do valor liquido'
  ,cod_camp_orig_rd  INT    COMMENT 'Codigo da campanha que deu origem ao desconto'
  ,cod_promo_pdv     INT    COMMENT 'Codigo da promocao no PDV'
) PARTITIONED BY (datamovto STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY', 'COMMENT'='Tabela de apuracao das RDs');