package ru.sberbank.sdcb.k7m.core.pack
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMainClass extends BaseMainClass {

   //---------------------------------------------------------
  override def run(params: Map[String, String], config: Config): Unit = {
    //---------------------------------------------------------

    val DevSchema = config.aux
    val MartSchema = config.pa

    //---------------------------------------------------------
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("ACC")
      .getOrCreate()

    val date = params.getOrElse("date", LoggerUtils.obtainRunDt(spark,config.aux).substring(0,10))

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

    val processLogger = new ProcessLogger(spark, config, "ACC") // processName- наименование процесса,
    processLogger.logStartProcess()

    val AccClient = (new AccClientClass(spark, config)).DoAccClient()

    val AccProduct = (new AccProductClass(spark, config, date)).DoAccProduct()

    val AccSvod = (new AccSvodClass(spark, config)).DoAccSvod()

    val Acc = (new AccClass(spark, config, date)).DoAcc()

    processLogger.logEndProcess()
  }
}


