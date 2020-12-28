#!/bin/bash
sh /projetos/plfinanceiro/bin/environment_variables.sh
source /projetos/plfinanceiro/bin/environment_variables.sh
impala-shell -k -i brgpalnxslp006.gpa.sl -B -q "DROP TABLE IF EXISTS rfzd_financeiro.margem_vendas_online PURGE;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.margem_vendas_online AS
        select 
        x.dat_venda
        ,x.hora
        ,x.codloja loja
        --,x.codloja+"-"+y.NM_Loja as loja_nmLoja
        ,x.nom_categ
        ,x.nom_subcateg
        ,y.NM_BU AS NM_BU
        ,y.NM_Bandeira AS NM_Bandeira
        ,y.NM_Regiao AS NM_Regiao
        ,y.NM_MicroRegiao AS NM_MicroRegiao
        ,y.NM_Loja as NM_Loja
        ,y.cod_uf_loja as UF

        ,'AUXILIAR' auxiliar

        --,x.codinternoproduto   cod_plu
        --,x.nom_prod
        ,sum(x.qtdeitemvenda)  qtd_itens
        ,sum(x.valorvenda)     valorvenda
        ,sum(x.valordesc)      desconto
        ,sum(x.venda_brt)      venda_bruta
        ,sum(x.val_custo_real) val_custo
        ,sum(x.valor_imposto)  val_imposto
        ,sum(x.venda_liq)      venda_liq
        ,sum(x.venda_liq - x.val_custo_real)  marg_valor
        --,sum((x.venda_liq - x.val_custo_real)/ )     marg_perc
    from (select from_unixtime(cast((dthriniciovenda / 1000) as bigint), 'yyyy/MM/dd') dat_venda
          ,from_unixtime(cast((dthriniciovenda / 1000) as bigint), 'HH')         hora
          ,cast(c.codloja as INT) as codloja
          ,i.codinternoproduto
          ,i.numseqoperacao
          ,i.numseqitem
          ,p.nom_prod
          ,cat.nom_categ
          ,sct.nom_subcateg
          ,i.aliquotapis
          ,i.aliquotacofins
          ,i.valoraliquotatrib
          ,i.qtdeitemvenda
          ,i.valorvenda
          ,i.valordesc
          ,i.valorcusto
          --,(i.valorprecounitario * i.qtdeitemvenda) venda_real
          ,(coalesce(ge01.cmv, i.valorcusto) * i.qtdeitemvenda) val_custo_real
          /*calculo de impostos*/
          ,(((i.aliquotapis + i.aliquotacofins + i.valoraliquotatrib)/100) * (i.valorvenda - i.valordesc)) valor_imposto
          /*Calculo de venda liquida = Venda bruta - impostos(pis, cofins, icms)*/
          ,(i.valorvenda  - i.valordesc) venda_brt
          ,((i.valorvenda - i.valordesc) - ((i.valorvenda - i.valordesc) * ((i.aliquotapis + i.aliquotacofins + i.valoraliquotatrib)/100))) venda_liq
          /*Dinheiro Margem = Venda Liquida - CMV*/
          ,(i.valorvenda - (coalesce(ge01.cmv, i.valorcusto) * i.qtdeitemvenda)) marg_dinheiro
          /*% Margem = Venda Bruta - Margem Dinheiro*/
          ---,((((i.valorvenda) - (coalesce(ge01.cmv, i.valorcusto) * i.qtdeitemvenda)) / i.valorvenda))  marg_perc
    from  siac_store.cupons c
          inner join--select * from
          siac_store.cuponsitens i
       on (    c.datamovto            = i.datamovto
           and c.codloja            = i.codloja
           and c.numterminal          = i.numterminal
           and c.numcontadorreiniciooperacao  = i.numcontadorreiniciooperacao
           and c.numseqoperacaoentrada      = i.numseqoperacaoentrada
           and c.numseqoperacao       = i.numseqoperacao
          )

          LEFT JOIN (
            SELECT 
             t1.cod_loja
            ,t1.cod_plu
            ,t1.dat_movto
            ,t1.val_unit_custo_estq_final as cmv
            from rfzd_financeiro.apur_cmv_ge01 t1
            INNER JOIN ( 
            SELECT
             max(dat_movto) as dat_movto
            ,cod_loja
            ,cod_plu
            FROM rfzd_financeiro.apur_cmv_ge01  
            GROUP BY cod_loja, cod_plu) t2 ON
            t1.cod_loja = t2.cod_loja AND
            t1.cod_plu = t2.cod_plu AND
            t1.dat_movto = t2.dat_movto ) ge01 on 
          i.codloja = ge01.cod_loja
          AND i.codinternoproduto = ge01.cod_plu

          inner join
          tszd_planej_coml.t_prod p
       on cast (i.codinternoproduto as int) = CAST(p.cod_plu as INT)
          inner join
        tszd_planej_coml.t_subgrupo sg
     on cast(p.cod_subgrupo as INT) = CAST(sg.cod_subgrupo as INT)
    and cast(p.cod_grupo as int)    = CAST(sg.cod_grupo as int)
    and cast (p.cod_secao as int)   = cast(sg.cod_secao as Int)
        inner join
        tszd_planej_coml.t_grupo g
     on cast(sg.cod_grupo as int) = cast(g.cod_grupo as int)
    and cast(sg.cod_secao as int) = cast(g.cod_secao as int)
        inner join
        rwzd_ac01.ext_td_ac01_dsdpm190_tsecao19 sec
     on cast(p.cod_secao as int)      = CAST(sec.cod_secao as INT)
     --and sec.data_referencia = 19000101
      inner join
      tszd_planej_coml.t_subcateg sct
     on cast(g.cod_subcateg as INT) = cast(sct.cod_subcateg as INT)
        inner join
        tszd_planej_coml.t_categ cat
     on cast(sct.cod_categ as int)   = cast(cat.cod_categ as int)
    where c.datamovto = unix_timestamp(SUBSTR(CAST(from_utc_timestamp(from_unixtime(cast(now() as bigint), 'yyyy-MM-dd HH:mm:ss'), 'Etc/GMT-3') AS STRING),1,10), 'yyyy-MM-dd') * 1000
          --and i.codinternoproduto = 1027306 --(BONAFONTE - 315449, Pão Vanderleia - 32537, CERVEJA - 1027306)
          --and c.codloja           = 5267
          and c.codtipocupom      = 1
          and i.codtipooperacao   = 0
          --and i.codsecao          = 10
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
    order by 2 desc
  ) x
    LEFT JOIN (
    SELECT
    CAST(t_loja.cod_estr_oper AS INT) as cod_loja
    ,t_loja.nome_estr_oper as NM_Loja
    ,t_micro.cod_estr_oper as cod_micro
    ,t_micro.nome_estr_oper as NM_MicroRegiao
    ,t_reg.cod_estr_oper as cod_reg
    ,t_reg.nome_estr_oper as NM_Regiao
    ,t_band.cod_estr_oper as cod_band
    ,t_band.nome_estr_oper as NM_Bandeira
    ,t_bu.cod_estr_oper as cod_bu
    ,t_bu.nome_estr_oper as NM_BU
    ,t_local.cod_uf_loja as cod_uf_loja
    FROM rwzd_ac51.estrutura_operacional as t_loja
    INNER JOIN (
    SELECT
    t_micro.cod_estr_oper
    ,t_micro.nome_estr_oper
    ,t_micro.cod_estr_oper_pai
    ,max(t_micro.idt_versao)
    FROM rwzd_ac51.estrutura_operacional t_micro
    WHERE cod_tipo_estr_oper = 7
    AND num_nive_estr_oper = 5
    AND idt_versao = (SELECT max(idt_versao) FROM rwzd_ac51.estrutura_operacional WHERE cod_tipo_estr_oper = 7)
    GROUP BY 1,2,3
    ) t_micro ON
    t_loja.cod_estr_oper_pai = t_micro.cod_estr_oper
    INNER JOIN (
    SELECT
    t_reg.cod_estr_oper
    ,t_reg.nome_estr_oper
    ,t_reg.cod_estr_oper_pai
    ,max(t_reg.idt_versao)
    FROM rwzd_ac51.estrutura_operacional t_reg
    WHERE cod_tipo_estr_oper = 7
    AND num_nive_estr_oper = 4
    AND idt_versao = (SELECT max(idt_versao) FROM rwzd_ac51.estrutura_operacional WHERE cod_tipo_estr_oper = 7)
    GROUP BY 1,2,3
    ) t_reg ON
    t_micro.cod_estr_oper_pai = t_reg.cod_estr_oper
    INNER JOIN (
    SELECT
    t_band.cod_estr_oper
    ,t_band.nome_estr_oper
    ,t_band.cod_estr_oper_pai
    ,max(t_band.idt_versao)
    FROM rwzd_ac51.estrutura_operacional t_band
    WHERE cod_tipo_estr_oper = 7
    AND num_nive_estr_oper = 3
    AND idt_versao = (SELECT max(idt_versao) FROM rwzd_ac51.estrutura_operacional WHERE cod_tipo_estr_oper = 7)
    GROUP BY 1,2,3
    ) t_band ON
    t_reg.cod_estr_oper_pai = t_band.cod_estr_oper
    INNER JOIN (
    SELECT
    t_bu.cod_estr_oper
    ,t_bu.nome_estr_oper
    FROM rwzd_ac51.estrutura_operacional t_bu
    WHERE cod_tipo_estr_oper = 7
    AND num_nive_estr_oper = 2
    AND idt_versao = (SELECT max(idt_versao) FROM rwzd_ac51.estrutura_operacional WHERE cod_tipo_estr_oper = 7)
    GROUP BY 1,2
    ) t_bu ON
    t_band.cod_estr_oper_pai = t_bu.cod_estr_oper
    INNER JOIN (
    SELECT
    cast(cod_loja as int) AS cod_loja
    ,cod_uf_loja
    FROM rwzd_ac01.td_ac01_dsdpm070_tlocal07 t_aux
    WHERE data_referencia = (
    SELECT
    MAX(data_referencia)
    FROM rwzd_ac01.td_ac01_dsdpm070_tlocal07 t_aux2
    WHERE t_aux.cod_loja = t_aux2.cod_loja)
    AND num_sequencial = (
    SELECT
    MAX(num_sequencial)
    FROM rwzd_ac01.td_ac01_dsdpm070_tlocal07 t_aux3
    WHERE t_aux.cod_loja = t_aux3.cod_loja
    AND   t_aux.data_referencia = t_aux3.data_referencia)) t_local ON
    CAST(t_loja.cod_estr_oper AS INT) = CAST(t_local.cod_loja AS INT)
    WHERE
    t_loja.cod_tipo_estr_oper = 7 AND
    t_loja.num_nive_estr_oper = 6 AND
    t_loja.idt_versao = (
    SELECT MAX(idt_versao)
    FROM rwzd_ac51.estrutura_operacional
    WHERE cod_tipo_estr_oper = 7)
    ) y ON
    CAST(x.codloja AS INT) = CAST(y.cod_loja AS INT)
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;"

impala-shell -k -i brgpalnxslp006.gpa.sl -B -q "DROP TABLE IF EXISTS rfzd_financeiro.margem_vendas_online_plu PURGE;
CREATE TABLE IF NOT EXISTS rfzd_financeiro.margem_vendas_online_plu AS
select x.dat_venda
        ,x.hora
        ,x.codloja loja
        --,x.codloja+"-"+y.NM_Loja as loja_nmLoja
        ,x.cod_categ
        ,x.nom_categ
        ,x.cod_subcateg
        ,x.nom_subcateg
        ,x.cod_grupo
        ,x.nom_grupo
        ,x.cod_subgrupo
        ,x.nom_sub_grupo
        ,y.NM_BU AS NM_BU
        ,y.NM_Bandeira AS NM_Bandeira
        ,y.NM_Regiao AS NM_Regiao
        ,y.NM_MicroRegiao AS NM_MicroRegiao
        ,y.NM_Loja as NM_Loja
        ,y.cod_uf_loja as UF
        ,y.cod_multivarejo 
        ,y.desc_multivarejo 
        ,x.codinternoproduto   cod_plu
        ,x.nom_prod
        ,sum(x.qtdeitemvenda)  qtd_itens
        ,sum(x.valorvenda)     valorvenda
        ,sum(x.valordesc)      desconto
        ,sum(x.venda_brt)      venda_bruta
        ,sum(x.val_custo_real) val_custo
        ,sum(x.valor_imposto)  val_imposto
        ,sum(x.venda_liq)      venda_liq
        ,sum(x.venda_liq - x.val_custo_real)  marg_valor
        --,sum((x.venda_liq - x.val_custo_real)/ )     marg_perc
    from (select from_unixtime(cast((dthriniciovenda / 1000) as bigint), 'yyyy/MM/dd') dat_venda
          ,from_unixtime(cast((dthriniciovenda / 1000) as bigint), 'HH')         hora
          ,cast(c.codloja as INT) as codloja
          ,i.codinternoproduto
          ,i.numseqoperacao
          ,i.numseqitem
          ,p.nom_prod
          ,cat.cod_categ
          ,cat.nom_categ
          ,sct.cod_subcateg
          ,sct.nom_subcateg
          ,g.cod_grupo
          ,g.nom_grupo
          ,sg.cod_subgrupo
          ,sg.nom_sub_grupo
          ,i.aliquotapis
          ,i.aliquotacofins
          ,i.valoraliquotatrib
          ,i.qtdeitemvenda
         ,i.valorvenda
          ,i.valordesc
          ,coalesce(ge01.cmv, i.valorcusto)
          --,(i.valorprecounitario * i.qtdeitemvenda) venda_real
          ,(coalesce(ge01.cmv, i.valorcusto) * i.qtdeitemvenda) val_custo_real
          /*calculo de impostos*/
          ,(((i.aliquotapis + i.aliquotacofins + i.valoraliquotatrib)/100) * (i.valorvenda - i.valordesc)) valor_imposto
          /*Calculo de venda liquida = Venda bruta - impostos(pis, cofins, icms)*/
          ,(i.valorvenda  - i.valordesc) venda_brt
         ,((i.valorvenda - i.valordesc) - ((i.valorvenda - i.valordesc) * ((i.aliquotapis + i.aliquotacofins + i.valoraliquotatrib)/100))) venda_liq
          /*Dinheiro Margem = Venda Liquida - CMV*/
          ,(i.valorvenda - (coalesce(ge01.cmv, i.valorcusto) * i.qtdeitemvenda)) marg_dinheiro
          /*% Margem = Venda Bruta - Margem Dinheiro*/
          ---,((((i.valorvenda) - (coalesce(ge01.cmv, i.valorcusto) * i.qtdeitemvenda)) / i.valorvenda))  marg_perc
       from  siac_store.cupons c
          inner join--select * from
          siac_store.cuponsitens i
          
       on (    c.datamovto            = i.datamovto
           and c.codloja            = i.codloja
           and c.numterminal          = i.numterminal
           and c.numcontadorreiniciooperacao  = i.numcontadorreiniciooperacao
           and c.numseqoperacaoentrada      = i.numseqoperacaoentrada
           and c.numseqoperacao       = i.numseqoperacao
          )
          
          LEFT JOIN (
            SELECT 
             t1.cod_loja
            ,t1.cod_plu
            ,t1.dat_movto
            ,t1.val_unit_custo_estq_final as cmv
            from rfzd_financeiro.apur_cmv_ge01 t1
            INNER JOIN ( 
            SELECT
             max(dat_movto) as dat_movto
            ,cod_loja
            ,cod_plu
            FROM rfzd_financeiro.apur_cmv_ge01  
            GROUP BY cod_loja, cod_plu) t2 ON
            t1.cod_loja = t2.cod_loja AND
            t1.cod_plu = t2.cod_plu AND
            t1.dat_movto = t2.dat_movto ) ge01 ON 
          i.codloja = ge01.cod_loja
          AND i.codinternoproduto = ge01.cod_plu
          
          inner join
          (select * from
          tszd_planej_coml.t_prod where dat_fim =             21001231)  p
       on cast (i.codinternoproduto as int) = CAST(p.cod_plu as INT)
          inner join
        (select * from tszd_planej_coml.t_subgrupo where dat_fim = 21001231)  sg
     on cast(p.cod_subgrupo as INT) = CAST(sg.cod_subgrupo as INT)
    and cast(p.cod_grupo as int)    = CAST(sg.cod_grupo as int)
    and cast (p.cod_secao as int)   = cast(sg.cod_secao as Int)
        inner join
        (select * from tszd_planej_coml.t_grupo where dat_fim = 21001231) g
     on cast(sg.cod_grupo as int) = cast(g.cod_grupo as int)
    and cast(sg.cod_secao as int) = cast(g.cod_secao as int)
        inner join
        rwzd_ac01.ext_td_ac01_dsdpm190_tsecao19 sec
     on cast(p.cod_secao as int)      = CAST(sec.cod_secao as INT)
     --and sec.data_referencia = 19000101
      inner join
      (select * from tszd_planej_coml.t_subcateg where dat_fim = 21001231) sct
     on cast(g.cod_subcateg as INT) = cast(sct.cod_subcateg as INT)
        inner join
        (select * from tszd_planej_coml.t_categ where dat_fim = 21001231) cat
     on cast(sct.cod_categ as int)   = cast(cat.cod_categ as int)
    where c.datamovto = CAST(to_timestamp(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),'yyyy-MM-dd') AS BIGINT) * 1000
          --and i.codinternoproduto = 1027306 --(BONAFONTE - 315449, Pão Vanderleia - 32537, CERVEJA - 1027306)
          --and c.codloja           = 5267
          and c.codtipocupom      = 1
          and i.codtipooperacao   = 0
          --and i.codsecao          = 10
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,17, 18, 19, 20, 21, 22
    order by 2 desc
  ) x
    LEFT JOIN (
    SELECT
    CAST(t_loja.cod_estr_oper AS INT) as cod_loja
    ,t_loja.nome_estr_oper as NM_Loja
    ,t_micro.cod_estr_oper as cod_micro
    ,t_micro.nome_estr_oper as NM_MicroRegiao
    ,t_reg.cod_estr_oper as cod_reg
    ,t_reg.nome_estr_oper as NM_Regiao
    ,t_band.cod_estr_oper as cod_band
    ,t_band.nome_estr_oper as NM_Bandeira
    ,t_bu.cod_estr_oper as cod_bu
    ,t_bu.nome_estr_oper as NM_BU
    ,t_multivarejo.cod_estr_oper as cod_multivarejo
    ,t_multivarejo.nome_estr_oper as desc_multivarejo
    ,t_local.cod_uf_loja as cod_uf_loja
    FROM rwzd_ac51.estrutura_operacional as t_loja
    INNER JOIN (
    SELECT
    t_micro.cod_estr_oper
    ,t_micro.nome_estr_oper
    ,t_micro.cod_estr_oper_pai
    ,max(t_micro.idt_versao)
    FROM rwzd_ac51.estrutura_operacional t_micro
    WHERE cod_tipo_estr_oper = 7
    AND num_nive_estr_oper = 5
    AND idt_versao = (SELECT max(idt_versao) FROM rwzd_ac51.estrutura_operacional WHERE cod_tipo_estr_oper = 7)
    GROUP BY 1,2,3
    ) t_micro ON
    t_loja.cod_estr_oper_pai = t_micro.cod_estr_oper
    INNER JOIN (
    SELECT
    t_reg.cod_estr_oper
    ,t_reg.nome_estr_oper
    ,t_reg.cod_estr_oper_pai
    ,max(t_reg.idt_versao)
    FROM rwzd_ac51.estrutura_operacional t_reg
    WHERE cod_tipo_estr_oper = 7
    AND num_nive_estr_oper = 4
    AND idt_versao = (SELECT max(idt_versao) FROM rwzd_ac51.estrutura_operacional WHERE cod_tipo_estr_oper = 7)
    GROUP BY 1,2,3
    ) t_reg ON
    t_micro.cod_estr_oper_pai = t_reg.cod_estr_oper
    INNER JOIN (
    SELECT
    t_band.cod_estr_oper
    ,t_band.nome_estr_oper
    ,t_band.cod_estr_oper_pai
    ,max(t_band.idt_versao)
    FROM rwzd_ac51.estrutura_operacional t_band
    WHERE cod_tipo_estr_oper = 7
    AND num_nive_estr_oper = 3
    AND idt_versao = (SELECT max(idt_versao) FROM rwzd_ac51.estrutura_operacional WHERE cod_tipo_estr_oper = 7)
    GROUP BY 1,2,3
    ) t_band ON
    t_reg.cod_estr_oper_pai = t_band.cod_estr_oper
    INNER JOIN (
    SELECT
    t_bu.cod_estr_oper
    ,t_bu.nome_estr_oper
    ,t_bu.cod_estr_oper_pai
    FROM rwzd_ac51.estrutura_operacional t_bu
    WHERE cod_tipo_estr_oper = 7
    AND num_nive_estr_oper = 2
    AND idt_versao = (SELECT max(idt_versao) FROM rwzd_ac51.estrutura_operacional WHERE cod_tipo_estr_oper = 7)
    GROUP BY 1,2,3
    ) t_bu ON
    t_band.cod_estr_oper_pai = t_bu.cod_estr_oper
    INNER JOIN (
    SELECT
    t_multivarejo.cod_estr_oper
    ,t_multivarejo.nome_estr_oper
    FROM rwzd_ac51.estrutura_operacional t_multivarejo
    WHERE cod_tipo_estr_oper = 7
    AND num_nive_estr_oper = 1
    AND idt_versao = (SELECT max(idt_versao) FROM rwzd_ac51.estrutura_operacional WHERE cod_tipo_estr_oper = 7)
    GROUP BY 1,2
    ) t_multivarejo ON
    t_bu.cod_estr_oper_pai = t_multivarejo.cod_estr_oper
    INNER JOIN (
    SELECT
    cast(cod_loja as int) AS cod_loja
    ,cod_uf_loja
    FROM rwzd_ac01.td_ac01_dsdpm070_tlocal07 t_aux
    WHERE data_referencia = (
    SELECT
    MAX(data_referencia)
    FROM rwzd_ac01.td_ac01_dsdpm070_tlocal07 t_aux2
    WHERE t_aux.cod_loja = t_aux2.cod_loja)
    AND num_sequencial = (
    SELECT
    MAX(num_sequencial)
    FROM rwzd_ac01.td_ac01_dsdpm070_tlocal07 t_aux3
    WHERE t_aux.cod_loja = t_aux3.cod_loja
    AND   t_aux.data_referencia = t_aux3.data_referencia)) t_local ON
    CAST(t_loja.cod_estr_oper AS INT) = CAST(t_local.cod_loja AS INT)
    WHERE
    t_loja.cod_tipo_estr_oper = 7 AND
    t_loja.num_nive_estr_oper = 6 AND
    t_loja.idt_versao = (
    SELECT MAX(idt_versao)
    FROM rwzd_ac51.estrutura_operacional
    WHERE cod_tipo_estr_oper = 7)
    ) y ON
    CAST(x.codloja AS INT) = CAST(y.cod_loja AS INT)
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21;
    INVALIDATE METADATA rfzd_financeiro.margem_vendas_online_plu;"