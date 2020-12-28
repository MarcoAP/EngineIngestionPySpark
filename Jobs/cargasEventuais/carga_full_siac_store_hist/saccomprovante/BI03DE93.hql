Insert into siac_store_hist.ext_saccomprovante PARTITION(dtemissao)
Select
cast (codloja as string) codloja
,cast(iddocumento as string) iddocumento
,cast(codtipodocumento as string) codtipodocumento
,cast(cupomcodloja as string) cupomcodloja
,cast(cupomnumseqoperacao as string) cupomnumseqoperacao
,cast(qtdeitem as string) qtdeitem
,cast(valordesconto as string) valordesconto
,cast(valortotal as string) valortotal
,cast(from_unixtime(CAST((datamovtocupom / 1000) as bigint), 'yyyy-MM-dd') as STRING) datamovtocupom
,cast(numdocumento as string) numdocumento
,cast(numterminalcupom as string) numterminalcupom
,cast(codtipodevolucao as string) codtipodevolucao
,cast(descmotivotroca as string) descmotivotroca
,cast(numterminalutilizado as string) numterminalutilizado
,cast(from_unixtime(CAST((dtvalidade / 1000) as bigint), 'yyyy-MM-dd HH:mm:ss') as STRING) dtvalidade
,cast(codtipo as string) codtipo
,cast(desctipodevolucao as string) desctipodevolucao
,cast(idcomprovantesituacao as string) idcomprovantesituacao
,cast(codsituacao as string) codsituacao
,cast(descidentificacaousuario as string) descidentificacaousuario
,cast(nomeusuario as string) nomeusuario
,cast(from_unixtime(CAST((dtmovsituacao / 1000) as bigint), 'yyyy-MM-dd HH:mm:ss') as STRING) dtmovsituacao
,cast(numseqoperacaoutilizado as string)  numseqoperacaoutilizado
,cast(indicestorefanulado as string) indicestorefanulado
,cast(descmotivo as string) descmotivo
,cast(from_unixtime(CAST((dthremissaocomprovante / 1000) as bigint), 'yyyy-MM-dd HH:mm:ss') as STRING) dthremissaocomprovante
,cast(codoperadorpdv as string) codoperadorpdv
,cast(descconvertidobonusrecarga as string) descconvertidobonusrecarga
,cast(from_unixtime(CAST((dthrinsercao / 1000) as bigint), 'yyyy-MM-dd HH:mm:ss') as STRING) dthrinsercao
,cast(from_timestamp(current_timestamp(), 'yyyy-MM-dd') as STRING) dt_processamento
,cast(from_unixtime(CAST((dtemissao / 1000) as bigint), 'yyyy-MM-dd') as STRING) dtemissao
FROM siac_store.saccomprovante
