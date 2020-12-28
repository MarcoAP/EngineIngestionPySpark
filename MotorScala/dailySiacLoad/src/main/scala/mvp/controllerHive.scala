package mvp {

  import org.apache.spark.SparkContext
  import org.apache.spark.sql.hive.HiveContext
  import org.apache.spark.sql.DataFrame
  import org.apache.kudu.spark.kudu._
  import org.apache.spark.sql.functions.{lit, concat}
  import sys.process._
  import java.time.LocalDate

  class controllerHive(sparkContext: SparkContext, hiveContext: HiveContext) {
    var table = ""
    var sqlSelect = ""

    val getDate: LocalDate = java.time.LocalDate.now()
    val getYestarday: LocalDate = getDate.minusDays(1)
    val getMinus30Days: LocalDate = getDate.minusDays(30)
    val dtprocess: String = getDate.toString

    val sc: SparkContext = sparkContext
    val hive: HiveContext = hiveContext

    val database = "siac_store_hist"
    val databaseKudo = "siac_store"
    var siacHdfsDir = s"/gpa/rawzone/stg/$database"
    val master1 = "brgpalnxslp003.gpa.sl:7051"
    val master2 = "brgpalnxslp004.gpa.sl:7051"
    val master3 = "brgpalnxslp005.gpa.sl:7051"
    val master: String = Seq(master1, master2, master3).mkString(",")
    val timezone: String = getTimeZoneDiff

    def getTimeZoneDiff: String = {
      val query = "SELECT from_unixtime(cast(0 as BIGINT), 'HH')"
      val df_timezone = hive.sql(query).collect()
      val aux = df_timezone.map(_.toSeq.toArray)
      val diff = aux(0)(0).toString.toInt - 24
      diff.toString
    }

    def kudoToDataframe(table: String): DataFrame = {
      println(s"impala::$databaseKudo.$table")
      println(master)
      val df = hive.read.options(Map("kudu.master" -> master, "kudu.table" -> s"impala::$databaseKudo.$table")).kudu
      df
    }

    def dropSiacPartition(table: String, partitionName: String): Unit = {
      val dropQuery = s"ALTER TABLE $database.$table DROP IF EXISTS PARTITION ($partitionName>='${getMinus30Days.toString}')"
      println(dropQuery)
      hive.sql(dropQuery)
      (0 to 29).map(days => getMinus30Days.plusDays(days)).foreach { day => s"hdfs dfs -rm -r -skipTrash $siacHdfsDir/$table/$partitionName=$day" ! }
    }

    def kudoToHiveCupons(): Unit = {
      table = "ext_cupons"
      sqlSelect =
        s"""
           |SELECT
           | codloja
           |,numterminal
           |,numcontadorreiniciooperacao
           |,numseqoperacaoentrada
           |,numseqoperacao
           |,codtipocupom
           |,codtipovenda
           |,from_utc_timestamp(from_unixtime(cast(dthriniciovenda /1000 as BIGINT), 'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as dthriniciovenda
           |,from_utc_timestamp(from_unixtime(cast(dthrtotalizacao/1000 as BIGINT), 'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as dthrtotalizacao
           |,from_utc_timestamp(from_unixtime(cast(dthrfimvenda/1000 as BIGINT), 'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as dthrfimvenda
           |,codcategoriacliente
           |,codtipopbm
           |,codclientepbm
           |,nomeclientepbm
           |,idprimariacliente
           |,idsecundariacliente
           |,codempresaconveniada
           |,codorigempedido
           |,codtipoareafiscal
           |,qtdeitens
           |,qtdeitemvenda
           |,valorvenda
           |,qtdeitemcancel
           |,valorcancel
           |,valordesc
           |,valordescsubtotal
           |,valorencargo
           |,valortroco
           |,valorcopay
           |,qtdepontos
           |,codstatuspontos
           |,gtvenda
           |,gtcancel
           |,gtdesc
           |,numcep
           |,codoperador
           |,codsupervisor
           |,numidentclienteimpressa
           |,descnomecliente
           |,codprepedido
           |,valordescsacolinha
           |,numserieimpressora
           |,numcontadorcuponsfiscais
           |,contreducaoz
           |,gtinivenda
           |,gtinicancel
           |,gtinidesc
           |,from_utc_timestamp(from_unixtime(cast(dthrcancelamento/1000 as BIGINT), 'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as dthrcancelamento
           |,numtalao
           |,numregistropromocoes
           |,indicpagtounificado
           |,tipocancelamento
           |,indicacrescimorateio
           |,codsupervvendalimite
           |,tipodoccliente
           |,numdoccliente
           |,chavecupomfiscaleletronico
           |,numcupomfiscaleletronico
           |,numseriesatfiscal
           |,numnfce
           |,numserienfce
           |,modoemissao
           |,numprotocolo
           |,modelodocfiscal
           |,tipopagamento
           |,valtottribfed
           |,valtottribest
           |,valtottribmun
           |,cpfassociadonetpoints
           |,indictementregadomicilio
           |,valtotfrete
           |,respofertacartaofic
           |,codstatusnfegatewaysap
           |,valordescconvertidoembonus
           |,valorbonusrecarga
           |,taxabonusrecarga
           |,from_utc_timestamp(from_unixtime(cast(dthrinsercao/1000 as BIGINT), 'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as dthrinsercao
           |,valortotalfcp
           |,valortotalfcpsubsttribret
           |,null as aliquotafcpsubsttribret
           |,null as valoricmsfcpsubsttribret
           |,null as codmunicipiofg
           |,null as codlistaservico
           |,null as indicissnfce
           |,null as indicincentivofiscal
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as datamovto
           | FROM tmp_cupons
           |WHERE
           |from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
            """.stripMargin
      dropSiacPartition(table, "datamovto")
      val df = kudoToDataframe("cupons")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_cupons")

      val dataframe = hive.sql(sqlSelect)
      dataframe.write.format("parquet").partitionBy("datamovto").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_cupons")
    }

    def kudoToHiveCuponsItens(): Unit = {
      table = "ext_cuponsitens"
      sqlSelect =
        s"""
           |SELECT
           | codloja
           |,numterminal
           |,numcontadorreiniciooperacao
           |,numseqoperacaoentrada
           |,numseqoperacao
           |,numseqitem
           |,codtipooperacao
           |,codinternoproduto
           |,codtipoproduto
           |,codvenda
           |,codsecao
           |,codvendedor
           |,codreserva
           |,qtdeitemvenda
           |,valorprecounitario
           |,valorcusto
           |,valorvenda
           |,valordesc
           |,valorcopay
           |,codtrib
           |,codlegendatrib
           |,valoraliquotatrib
           |,codmodovenda
           |,codcateriapromocao
           |,codtipoentrega
           |,numcrm
           |,numautorizpbms
           |,codsupervisor
           |,numregistradoricms
           |,codtipoicmsouiss
           |,valordescsubtotal
           |,indicpiscofinszero
           |,valorpis
           |,valorcofins
           |,percpis
           |,perccofins
           |,valoracrescimosubtotal
           |,codsupervoff
           |,unidade
           |,aliquotapis
           |,aliquotacofins
           |,codsupervexped
           |,csticms
           |,origemproduto
           |,cfop
           |,numlote
           |,codautorizreceita
           |,percaliquotacheiaicms
           |,percreducaobaseicms
           |,codcomissaovendedor
           |,valtribfed
           |,valtribest
           |,valtribmun
           |,cstpis
           |,cstcofins
           |,codanp
           |,cest
           |,valfreterateado
           |,aliquotapissubsttrib
           |,aliquotacofinssubsttrib
           |,valorbasepissubsttrib
           |,valorbasecofinssubsttrib
           |,valorimposto
           |,valorbaseimposto
           |,codcesta
           |,ncm
           |,valordescconvertidoembonus
           |,ppb
           |,flagproducaoescala
           |,cnpjfabricante
           |,codbeneficiofiscaluf
           |,aliquotaicmssemfcp
           |,aliquotafcp
           |,valorbasefcp
           |,valorfcp
           |,valorbaseicmssubsttribret
           |,aliquotaconsumidorfinal
           |,valoricmssubsttribret
           |,valorbasefcpsubsttribret
           |,aliquotafcpsubsttribret
           |,valoricmsfcpsubsttribret
           |,codmunicipiofg
           |,codlistaservico
           |,indicissnfce
           |,indicincentivofiscal
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as datamovto
           | FROM tmp_cuponsitens
           |WHERE
           |from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
        """.stripMargin
      dropSiacPartition(table, "datamovto")
      val df = kudoToDataframe("cuponsitens")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_cuponsitens")

      val dataframe = hive.sql(sqlSelect)
      dataframe.write.format("parquet").partitionBy("datamovto").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_cuponsitens")
    }

    def kudoToHiveDescBonusFidelidade(): Unit = {
      table = "ext_descbonusfidelidade"
      sqlSelect =
        s"""
           |SELECT
           | codloja
           |,numterminal
           |,numcontadorreiniciooperacao
           |,numseqoperacaoentrada
           |,numseqoperacao
           |,iddetdesconto
           |,qtdevenda
           |,valorprecooriginal
           |,valorprecopromoprod
           |,valordesconto
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as datamovto
           | FROM tmp_descbonusfidelidade
           |WHERE from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
            """.stripMargin

      val df = kudoToDataframe("descbonusfidelidade")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_descbonusfidelidade")

      dropSiacPartition(table, "datamovto")
      val dataframe = hive.sql(sqlSelect)
      dataframe.take(1)
      dataframe.write.format("parquet").partitionBy("datamovto").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_descbonusfidelidade")
    }

    def kudoToHiveDesconto(): Unit = {
      table = "ext_desconto"
      sqlSelect =
        s"""
           |SELECT
           | codloja
           |,numterminal
           |,numcontadorreiniciooperacao
           |,numseqoperacaoentrada
           |,numseqoperacao
           |,iddetdesconto
           |,codtipodesconto
           |,codtipocodigopromocao
           |,codpromocao
           |,codinternoproduto
           |,codvenda
           |,qtdeitemvenda
           |,valorprecounitario
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(dthrdesconto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as dthrdesconto
           |,codoperador
           |,descconvertidobonusrecarga
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as datamovto
           | FROM tmp_desconto
           | WHERE from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
            """.stripMargin

      val df = kudoToDataframe("desconto")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_desconto")

      dropSiacPartition(table, "datamovto")
      val dataframe = hive.sql(sqlSelect)
      dataframe.take(1)
      dataframe.write.format("parquet").partitionBy("datamovto").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_desconto")
    }

    def kudoToHiveDescValorOferta(): Unit = {
      table = "ext_descvaloroferta"
      sqlSelect =
        s"""
           |SELECT
           | codloja
           |,numterminal
           |,numcontadorreiniciooperacao
           |,numseqoperacaoentrada
           |,numseqoperacao
           |,iddetdesconto
           |,valordesconto
           |,valoroferta
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as datamovto
           | FROM tmp_descvaloroferta
           | WHERE from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
            """.stripMargin

      val df = kudoToDataframe("descvaloroferta")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_descvaloroferta")

      dropSiacPartition(table, "datamovto")
      val dataframe = hive.sql(sqlSelect)
      dataframe.take(1)
      dataframe.write.format("parquet").partitionBy("datamovto").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_descvaloroferta")
    }

    def kudoToHivePromocao(): Unit = {
      table = "ext_promocao"
      sqlSelect =
        s"""
           |SELECT
           | codloja
           |,numterminal
           |,numcontadorreiniciooperacao
           |,numseqoperacaoentrada
           |,numseqoperacao
           |,iddetpromocao
           |,codtipopromocao
           |,codtipocodigopromocao
           |,codpromocao
           |,codinternoproduto
           |,codvenda
           |,codcategoriaefetivada
           |,qtdeitemvenda
           |,valorprecounitario
           |,from_utc_timestamp(from_unixtime(cast(dthrpromocao/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as dthrpromocao
           |,codoperador
           |,descconvertidobonusrecarga
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as datamovto
           | FROM tmp_promocao
           | WHERE from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
            """.stripMargin

      val df = kudoToDataframe("promocao")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_promocao")

      dropSiacPartition(table, "datamovto")
      val dataframe = hive.sql(sqlSelect)
      dataframe.take(1)
      dataframe.write.format("parquet").partitionBy("datamovto").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_promocao")
    }

    def kudoToHivePromocaoDesc(): Unit = {
      table = "ext_promocaodesc"
      sqlSelect =
        s"""
           |SELECT
           | codloja
           |,numterminal
           |,numcontadorreiniciooperacao
           |,numseqoperacaoentrada
           |,numseqoperacao
           |,iddetpromocao
           |,valorprecounitariopromo
           |,valorbasedesc
           |,valorpercdesc
           |,valordescunitario
           |,valordesc
           |,codsupervisor
           |,codformaaplicacao
           |,numnsupdvgift
           |,numnsupdvfic
           |,codstatusbonusfic
           |,numseqcupomfic
           |,valorcompra
           |,valorcupomgift
           |,valorconfeccaocartao
           |,codstatusrecarga
           |,valorbonusfic
           |,codsecao
           |,codgrupo
           |,codsubgrupo
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as datamovto
           | FROM tmp_promocaodesc
           |WHERE from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
            """.stripMargin

      val df = kudoToDataframe("promocaodesc")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_promocaodesc")

      dropSiacPartition(table, "datamovto")
      val dataframe = hive.sql(sqlSelect)
      dataframe.take(1)
      dataframe.write.format("parquet").partitionBy("datamovto").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_promocaodesc")
    }

    def kudoToHivePromocaoPackVirtual(): Unit = {
      table = "ext_promocaopackvirtual"
      sqlSelect =
        s"""
           |SELECT
           | codloja
           |,numterminal
           |,numcontadorreiniciooperacao
           |,numseqoperacaoentrada
           |,numseqoperacao
           |,iddetpromocao
           |,codsecao
           |,valordesc
           |,codlegendatrib
           |,flagprodutogratis
           |,qtdegratis
           |,codagrupamento
           |,codformaaplicacao
           |,qtdeitensformampack
           |,qtdeconjuntosformados
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT),'yyyy-MM-dd') as datamovto
           | FROM tmp_promocaopackvirtual
           |WHERE
           |from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT),'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
            """.stripMargin

      val df = kudoToDataframe("promocaopackvirtual")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_promocaopackvirtual")

      dropSiacPartition(table, "datamovto")
      val dataframe = hive.sql(sqlSelect)
      dataframe.take(1)
      dataframe.write.format("parquet").partitionBy("datamovto").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_promocaopackvirtual")
    }

    def kudoToHivePromocaoDescAtacado(): Unit = {
      table = "ext_promocaodescatacado"
      sqlSelect =
        s"""
           |SELECT
           | codloja
           |,numterminal
           |,numcontadorreiniciooperacao
           |,numseqoperacaoentrada
           |,numseqoperacao
           |,iddetpromocao
           |,valordesc
           |,codsecao
           |,codtipopremio
           |,codtipoaplicacao
           |,codgrupo
           |,codlegendatrib
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as datamovto
           | FROM tmp_promocaodescatacado
           | WHERE from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
            """.stripMargin

      val df = kudoToDataframe("promocaodescatacado")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_promocaodescatacado")

      dropSiacPartition(table, "datamovto")
      val dataframe = hive.sql(sqlSelect)
      dataframe.take(1)
      dataframe.write.format("parquet").partitionBy("datamovto").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_promocaodescatacado")
    }

    def kudoToHiveSacComprovanteProduto(): Unit = {
      table = "ext_saccomprovanteproduto"
      sqlSelect =
        s"""
           |SELECT
           | codloja
           |,iddocumento
           |,numitem
           |,codinternoproduto
           |,codvendaproduto
           |,descproduto
           |,descunidade
           |,descembalagem
           |,qtdeitem
           |,valorprecovendaunitplu
           |,valorprecocustounitplu
           |,valordesconto
           |,valortotal
           |,codlegendatrib
           |,valoraliquotatrib
           |,codsituacaotributaria
           |,codsecao
           |,codgrupo
           |,codsubgrupo
           |,codvendedor
           |,codreserva
           |,indicdevolvidonaloja
           |,valorpis
           |,valorcofins
           |,valoraliquotapis
           |,valoraliquotacofins
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(dtemissao/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as dtemissao
           | FROM tmp_saccomprovanteproduto
           | WHERE from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(dtemissao/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
            """.stripMargin

      val df = kudoToDataframe("saccomprovanteproduto")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_saccomprovanteproduto")

      dropSiacPartition(table, "dtemissao")
      val dataframe = hive.sql(sqlSelect)
      dataframe.take(1)
      dataframe.write.format("parquet").partitionBy("dtemissao").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_saccomprovanteproduto")
    }
    def kudoToHiveMr3(): Unit = {
      table = "apur_rd"
      sqlSelect =
        s"""select
           |cast(codloja as INT) as cod_loja
           |,codproduto as cod_plu
           |,cast(nroterminal as INT) as num_term
           |,nrocupom as num_cupom
           |,cast(codtipord as INT) as cod_tpo_rd
           |,cast(codtipooferta as INT) as cod_tpo_ofer
           |,cast(codsubtipooferta as INT) as cod_stpo_ofer
           |,valorrd as val_aplic_rd
           |,valorrdliquida as val_liq_aplic_rd
           |,fatorcalcvendaliq as val_fator_calc_imp
           |,codcampanhaorig as cod_camp_orig_rd
           |,codpromocaopdv as cod_promo_pdv
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as datamovto
           | from tmp_tszd_mr3
           | WHERE from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(datamovto/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') =
           | '${getYestarday.toString}'""".stripMargin


      val database_tszd= "tszd_financeiro"
      val df = kudoToDataframe("mr3")
      df.registerTempTable("tmp_tszd_mr3")
      val mr3 = hive.sql(sqlSelect)

      if (mr3.take(1).length > 0) {
        mr3.write.format("parquet").partitionBy("datamovto").mode("append").insertInto(s"$database_tszd.$table")
        hive.dropTempTable("tmp_tszd_mr3")
      }
    }

    def kudoToHiveSacComprovante(): Unit = {
      table = "ext_saccomprovante"
      sqlSelect =
        s"""
           |Select
           |codloja
           |,iddocumento
           |,codtipodocumento
           |,cupomcodloja
           |,cupomnumseqoperacao
           |,qtdeitem
           |,valordesconto
           |,valortotal
           |,from_utc_timestamp(from_unixtime(cast(datamovtocupom/1000 as BIGINT),'yyyy-MM-dd'),'Etc/GMT$timezone') as datamovtocupom
           |,numdocumento
           |,numterminalcupom
           |,codtipodevolucao
           |,descmotivotroca
           |,numterminalutilizado
           |,from_utc_timestamp(from_unixtime(cast(dtvalidade/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as dtvalidade
           |,codtipo
           |,desctipodevolucao
           |,idcomprovantesituacao
           |,codsituacao
           |,descidentificacaousuario
           |,nomeusuario
           |,from_utc_timestamp(from_unixtime(cast(dtmovsituacao/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as dtmovsituacao
           |,numseqoperacaoutilizado
           |,indicestorefanulado
           |,descmotivo
           |,from_utc_timestamp(from_unixtime(cast(dthremissaocomprovante/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as dthremissaocomprovante
           |,codoperadorpdv
           |,descconvertidobonusrecarga
           |,from_utc_timestamp(from_unixtime(cast(dthrinsercao/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as dthrinsercao
           |,dt_processamento
           |,from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(dtemissao/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as dtemissao
           | FROM tmp_saccomprovante
           | WHERE from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(dtemissao/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') BETWEEN '${getMinus30Days.toString}' AND '${getYestarday.toString}'
            """.stripMargin

      val df = kudoToDataframe("saccomprovante")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_saccomprovante")

      dropSiacPartition(table, "dtemissao")
      val dataframe = hive.sql(sqlSelect)
      dataframe.take(1)
      dataframe.write.format("parquet").partitionBy("dtemissao").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_saccomprovante")
    }

    def kudoToHiveSacCoberturaProduto(): Unit = {
      table = "ext_saccobertura_produto"
      sqlSelect = 
        s"""
            |select
            |codloja,
            |idcobertura,
            |idcoberturaproduto,
            |qtdcoberturaproduto,
            |desccoberturaproduto,
            |descmotivodivergencia,
            |vlrunitario,
            |vlrunitarioplu,
            |vlrdescontoadicional,
            |vlrcobertura,
            |codinterno,
            |codtipoproduto,
            |codsecao,
            |descsecao,
            |descproduto,
            |codlegendatrib,
            |valoraliquotatrib,
            |nomeconcorrente,
            |codconcorrente,
            |valorpis,
            |valorcofins,
            |valoraliquotapis,
            |valoraliquotacofins,
            |dt_processamento,
            |from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(dtemissao/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') as dtemissao
            |FROM tmp_saccobertura_produto
            |WHERE from_unixtime(cast(from_utc_timestamp(from_unixtime(cast(dtemissao/1000 as BIGINT),'yyyy-MM-dd HH:mm:ss'),'Etc/GMT$timezone') as BIGINT), 'yyyy-MM-dd') = '2019-05-18'
            """.stripMargin
            
      val df = kudoToDataframe("saccoberturaproduto")
      val newDF = df.withColumn("dt_processamento", lit(dtprocess))
      newDF.registerTempTable("tmp_saccobertura_produto")

      dropSiacPartition(table, "dtemissao")
      val dataframe = hive.sql(sqlSelect)
      dataframe.take(1)
      dataframe.write.format("parquet").partitionBy("dtemissao").mode("append").insertInto(s"$database.$table")
      hive.dropTempTable("tmp_saccobertura_produto")
    }

  }
}