package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object SparkMainClass extends BaseMainClass {

  //---------------------------------------------------------

  val SourceSchemaCRMcb = "internal_crm_cb_siebel"

  val targTableNameKM = ".K7M_KM"

  val OrgOuTypeCd = "'территория'"

  val OrgCustStatCd = "'архив'"

  val CvrgRoleCd = "'КЛИЕНТСКИЙ МЕНЕДЖЕР','КУРАТОР ЦА','КУРАТОР КП ЦА'"

  val applicationName = "KM"

  override def run(params: Map[String, String], config: Config): Unit = {

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

    val CrmOrgDF =   new CrmOrgClass(spark, config)
    val CrmUserDF = new CrmUserClass(spark, config)
    val GtPstnDF = new GtPstnClass(spark, config)
    val CrmCustTeamDF = new CrmCustTeamClass(spark, config)
    val CrmPositionDF = new CrmPositionClass(spark, config)
    val CrmOrgStructDF = new CrmOrgStructClass(spark, config)
    val KMDF = new KMClass(spark, config)

    CrmOrgDF.DoCrmOrg()
    CrmUserDF.DoCrmUser()
    GtPstnDF.DoGtPstn()
    CrmCustTeamDF.DoCrmCustTeam()
    CrmPositionDF.DoCrmPosition()
    CrmOrgStructDF.DoCrmOrgStruct()
    KMDF.DoKM()

    processLogger.logEndProcess()
  }
}
