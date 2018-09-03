package ru.sberbank.sdcb.k7m.core.pack
import java.time.LocalDate
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMainClass extends BaseMainClass {


  //---------------------------------------------------------
  override def run(params: Map[String, String], config: Config): Unit = {
    //---------------------------------------------------------

    val DevSchema = config.aux

    val MartSchema = config.pa

   // val date = params.getOrElse("date", "2018-01-01")


    //---------------------------------------------------------
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("Plan_liab")
      .getOrCreate()

    val date = params.getOrElse("date", LoggerUtils.obtainRunDt(spark,config.aux).substring(0,10))

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

    val processLogger = new ProcessLogger(spark, config, "Plan_liab") // processName- наименование процесса,
    processLogger.logStartProcess()

    val PrdDt = (new PrdDtClass(spark, config, date)).DoPrdDt()
    val HoaDt = (new HoaDtClass(spark, config, date)).DoHoaDt()
    val CarDt = (new CarDtClass(spark, config)).DoCarDt()
    val LimDt = (new LimDtClass(spark, config, date)).DolimDt()
    val CalDt = (new CalDtClass(spark, config, date)).DoCalDt()
    val ClientDebtOd = (new ClientDebtOdClass(spark, config)).DoClientDebtOd()

    processLogger.logEndProcess()
  }
}


