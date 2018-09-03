package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object SparkMainClass extends BaseMainClass {
  //---------------------------------------------------------

//  val SourceSchemaCRM = "internal_crm_cb_siebel"--используем нулевой слой
  val TableName = ".STG_GSZ_CRIT"
  val TableNameGSZ = ".STG_GSZ_CRMLK"

  //---------------------------------------------------------



  override def run(params: Map[String, String], config: Config): Unit = {

    val spark = SparkSession
      .builder()
        .enableHiveSupport()
      .appName("GSZCRM")
      .getOrCreate()
    val processLogger = new ProcessLogger(spark, config, "GSZCRM") // processName- наименование процесса,
    processLogger.logStartProcess()

    val SmartGSZ =   new DashboardClass(spark, config).DoSmartGSZ()

    processLogger.logEndProcess()
  }

}
