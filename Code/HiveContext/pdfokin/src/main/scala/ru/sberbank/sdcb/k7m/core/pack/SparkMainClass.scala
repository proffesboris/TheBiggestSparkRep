package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object SparkMainClass extends BaseMainClass {

  val STAGE_ID_KEY = "stageId"

  //---------------------------------------------------------
  override def run(params: Map[String, String], config: Config): Unit = {
    //---------------------------------------------------------

    val DevSchema = config.aux

    val MartSchema = config.pa

    //---------------------------------------------------------
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("pdfokin")
      .getOrCreate()

    val stageId = obtainStageId(params)

    if (stageId == "1" || stageId == "all") {

    }

    if (stageId == "2" || stageId == "all") {
      val PdFokIn = (new PdFokInClass(spark, config)).DoPdFokIn()
    }

  }

  def obtainStageId(params: Map[String, String]): String = {
    if (params.contains(STAGE_ID_KEY)) params(STAGE_ID_KEY) else throw new Exception("No stageId in parameters! Use stageId = 1 or 2")
  }
}


