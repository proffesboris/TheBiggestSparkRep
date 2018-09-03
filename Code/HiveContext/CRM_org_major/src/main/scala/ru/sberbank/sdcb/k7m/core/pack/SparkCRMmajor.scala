package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object SparkCRMmajor extends BaseMainClass {



  val excludeInn = "'0000000000'"

  val typeCd = "'ИП,Холдинг'"

  val CvrgRoleCd = "'КЛИЕНТСКИЙ МЕНЕДЖЕР','КУРАТОР ЦА','КУРАТОР КП ЦА'"

  val applicationName = "CRM_ORG_MAJOR"

  override def run(params: Map[String, String], config: Config): Unit = {
    //---------------------------------------------------------

    val Stg0Schema = config.stg

    val DevSchema = config.aux

    val MartSchema = config.pa

    //---------------------------------------------------------


    val spark = SparkSession
      .builder()
      .appName(applicationName)
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", "10000")
      .config("hive.exec.max.dynamic.partitions.pernode","10000")
      .getOrCreate()

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

    val processLogger = new ProcessLogger(spark, config, applicationName) // processName- наименование процесса,
    processLogger.logStartProcess()

    val OrgMjDF = new CRMOrgMjClass(spark, config)
    val OrgMajorDF = new CRMOrgMajorClass(spark, config)

    OrgMjDF.DoOrgMj()
    OrgMajorDF.DoOrgMajor()

    processLogger.logEndProcess()
  }
}
