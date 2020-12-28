CREATE DATABASE IF NOT EXISTS rfzv_financeiro;
CREATE VIEW IF NOT EXISTS rfzv_financeiro.mx2 AS 
SELECT * FROM rwzd_vb.ext_mx2
where cast(idt_operacao as int) = 1;