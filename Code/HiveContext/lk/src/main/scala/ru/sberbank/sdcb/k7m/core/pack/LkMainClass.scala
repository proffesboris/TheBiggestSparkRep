package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object LkMainClass extends BaseMainClass {

  val STAGE_ID_KEY = "stageId"
  val defLimitUp = "1000000"
  val defLimitDown = "-1"
  val SBRF_INN = "'7707083893'"
  val excludeINN = "'000%'"

  val applicationName = "LK"

  override def run(params: Map[String, String], config: Config): Unit = {

    val StageId = obtainStageId(params)

    val spark = SparkSession
      .builder
      .enableHiveSupport()
      .appName(s"${applicationName}_${StageId}")
      .getOrCreate

    val processLogger = new ProcessLogger(spark, config, applicationName) // processName- наименование процесса,
    processLogger.logStartProcess()

    if ((StageId =="all") || (StageId=="1")){
      val LKLKCRawDF = new LKLKCRawClass(spark,config).DoLKLKCRaw()
    }

    if ((StageId =="all") || (StageId=="2")){
      val LKLKCWithKeysDF = new LKLKCWithKeysClass(spark,config)
      val LKLKCDF = new LKLKCClass(spark,config)
      val LKDF = new LKClass(spark,config)
      val LKCDF = new LKCClass(spark,config)


      LKLKCWithKeysDF.DoLKLKCWithKeys()
      LKLKCDF.DoLKLKC()
      LKDF.DoLK()
      LKCDF.DoLKC()

      processLogger.logEndProcess()
    }
  }
  def obtainStageId(params: Map[String, String]): String = {
    if (params.contains(STAGE_ID_KEY)) params(STAGE_ID_KEY) else throw new Exception("No stageId in parameters. Use stageId = 1 or 2 ")
  }
}
