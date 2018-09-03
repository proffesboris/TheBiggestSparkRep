package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object SparkMain extends BaseMainClass {

  override def run(params: Map[String, String], config: Config): Unit = {

    val ListKey = "client_list"

    val spark = SparkSession
      .builder()
      .appName("VDCRMOut")
      .enableHiveSupport()
      .getOrCreate()

    val client_list: Int = params.getOrElse(ListKey,"0").toInt

    val processLogger = new ProcessLogger(spark, config, s"VDCRMOut")
    processLogger.logStartProcess()

    new VDCRMOut(spark, config).run(client_list)

    processLogger.logEndProcess()
  }

}
