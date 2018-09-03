package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object SparkMainClass extends BaseMainClass {


  val OrgOuTypeCd = "'территория'"

  val OrgCustStatCd = "'архив'"

  val CvrgRoleCd = "'КЛИЕНТСКИЙ МЕНЕДЖЕР','КУРАТОР ЦА','КУРАТОР КП ЦА'"

  val applicationName = "CRM_BASE"

  override def run(params: Map[String, String], config: Config): Unit = {
    //---------------------------------------------------------

    val Stg0Schema = config.stg

    val DevSchema = config.aux

    val MartSchema = config.pa

    //---------------------------------------------------------


    val spark = SparkSession
      .builder()
        .enableHiveSupport()
      .appName(applicationName)
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", "10000")
      .config("hive.exec.max.dynamic.partitions.pernode","10000")
      .getOrCreate()

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

    val processLogger = new ProcessLogger(spark, config, applicationName) // processName- наименование процесса,
    processLogger.logStartProcess()

    val CritDF = new CritClass(spark, config)
    val CRMLKDF = new CRMLKClass(spark, config)
    val CRMFilialDF = new CRMFilialClass(spark, config)
    val CRMOrgBaseDF = new CRMOrgBaseClass(spark, config)
    val CRMOrgStructDF = new CRMRepOrgstructuresClass(spark, config)

    CritDF.DoCRIT()
    CRMLKDF.DoCRMLK()
    CRMFilialDF.DoCRMFilial()
    CRMOrgBaseDF.DoCRMOrgBase()
    CRMOrgStructDF.DoCRMRepOrgStruct()

    processLogger.logEndProcess()
  }
}
