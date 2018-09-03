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
      .appName("pdintegrumin")
      .getOrCreate()

    val paramDate = params.getOrElse("date", LoggerUtils.obtainRunDtWithoutTime(spark,config.aux))
    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

    val pdintegrumin = (new Integrum7mClass(spark, config)).DoPdIntegrumIn(paramDate)

  }
}


