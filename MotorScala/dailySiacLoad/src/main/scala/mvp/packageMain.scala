package mvp {
  import org.apache.spark.sql.SQLContext
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.sql.hive.HiveContext

  object packageMain {
    def main(args: Array[String]): Unit = {

      if (args.length != 0) {
        val option = Integer.parseInt(args(0))

        val conf = new SparkConf()
        conf.setAppName("[Squad FI] Motor de Ingestao Diario - SIAC")
        //conf.set("spark.driver.allowMultipleContexts", "true")

        val sparkContext = new SparkContext(conf)
        //val sqlContext = new SQLContext(sparkContext)
        val hiveContext = new HiveContext(sparkContext)
        hiveContext.setConf("hive.exec.dynamic.partition", "true")
        hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

        val hive = new controllerHive(sparkContext, hiveContext)

        if (option == 1) {
          hive.kudoToHiveCupons()
        }
        else if (option == 2) {
          hive.kudoToHiveCuponsItens()
        }
        else if (option == 3) {
          hive.kudoToHiveDescBonusFidelidade()
        }
        else if (option == 4) {
          hive.kudoToHiveDesconto()
        }
        else if (option == 5) {
          hive.kudoToHiveDescValorOferta()
        }
        else if (option == 6) {
          hive.kudoToHivePromocao()
        }
        else if (option == 7) {
          hive.kudoToHivePromocaoDesc()
        }
        else if (option == 9) {
          hive.kudoToHivePromocaoPackVirtual()
        }
        else if (option == 8) {
          hive.kudoToHivePromocaoDescAtacado()
        }
        else if (option == 10) {
          hive.kudoToHiveSacComprovanteProduto()
        }
        else if (option == 11){
          hive.kudoToHiveMr3()
        }
        else if (option == 12){
          hive.kudoToHiveSacComprovante()
        }
        else if (option == 13){
          hive.kudoToHiveSacCoberturaProduto()
        }

      }
    }
  }

}