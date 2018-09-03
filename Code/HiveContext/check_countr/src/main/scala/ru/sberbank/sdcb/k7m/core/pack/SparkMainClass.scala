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
      .appName("check_countr")
      .getOrCreate()

    val date = params.getOrElse("date", LoggerUtils.obtainRunDtWithoutTime(spark,config.aux))

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

    val processLogger = new ProcessLogger(spark, config, "check_countr") // processName- наименование процесса,
    processLogger.logStartProcess()

    val CountrStg1 = (new CountrStg1Class(spark, config)).DoCountrStg1()

    val CountrStg2 = (new CountrStg2Class(spark, config)).DoCountrStg2()

    val CountrStg3 = (new CountrStg3Class(spark, config)).DoCountrStg3(date)

    val CountrStg4 = (new CountrStg4Class(spark, config)).DoCountrStg4()

    val CountrStg5 = (new CountrStg5Class(spark, config)).DoCountrStg5()

    val CheckCountr = (new CheckCountrClass(spark, config)).DoCheckCountr()

    processLogger.logEndProcess()
  }
}


