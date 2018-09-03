package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object SparkMainClass extends BaseMainClass {


  //---------------------------------------------------------

  val dateString = "2018-02-01"

  val OrgOuTypeCd = "'территория'"

  val OrgCustStatCd = "'архив'"

  val CvrgRoleCd = "'КЛИЕНТСКИЙ МЕНЕДЖЕР','КУРАТОР ЦА','КУРАТОР КП ЦА'"

  var recordsWrittenCount = 0L

  val applicationName = "EKS_ID"
  //---------------------------------------------------------

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

    val paramDate = params.getOrElse("date",LoggerUtils.obtainRunDtWithoutTime(spark,config.aux))

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

    val processLogger = new ProcessLogger(spark, config, applicationName) // processName- наименование процесса,
    processLogger.logStartProcess()

    val RDMTBMappingDF = (new RDMTBMappingClass(spark, config))
    val CluExtBasisDF = (new CluExtendedBasisClass(spark, config))
    val CluToEksIdDF = (new CluToEksIdClass(spark, config))
    val CluBaseDF = (new CluBaseClass(spark, config))
    val EIOFilterLkLkc =(new EIOFilterLkLkc(spark, config))

    RDMTBMappingDF.DoRDMTBMapping()
    CluExtBasisDF.DoCLUExtBasis()
    CluToEksIdDF.DoCluToEksId(paramDate)
    CluBaseDF.DoCLUBase()
    EIOFilterLkLkc.DoEIOSelected()
    processLogger.logEndProcess()
  }
}
