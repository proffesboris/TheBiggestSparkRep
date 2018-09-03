package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object ExsGenMain extends BaseMainClass{

  val dateDefault: String = s"""2018-02-02"""

  val mainVId: String = s"""91414%"""

  val clPriv: String = s"""CL_PRIV"""


	override def run(params: Map[String, String], config: Config): Unit = {

    val spark = SparkSession
      .builder()
      .appName("EXSGen")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", "10000")
      .config("hive.exec.max.dynamic.partitions.pernode","10000")
      .getOrCreate()

      val processLogger = new ProcessLogger(spark, config, "EXSGen") // processName- наименование процесса,
      processLogger.logStartProcess()

      val paramDate = params.getOrElse("date",LoggerUtils.obtainRunDtWithoutTime(spark,config.aux))

      new exsGenClass(spark,config).DoEXSGen(paramDate)

      processLogger.logEndProcess()

	}


}
