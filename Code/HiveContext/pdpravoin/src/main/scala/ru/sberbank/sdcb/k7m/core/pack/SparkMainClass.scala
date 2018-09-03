package ru.sberbank.sdcb.k7m.core.pack
import java.time.LocalDate
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
      .appName("pdpravoin")
      .getOrCreate()

    val paramDate = params.getOrElse("date", LoggerUtils.obtainRunDtWithoutTime(spark,config.aux))

 //   val paramDate = "2018-04-29"

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

    val pdpravoin = (new PdPravoInClass(spark, config)).DoPdPravoIn(paramDate)

  }
}


