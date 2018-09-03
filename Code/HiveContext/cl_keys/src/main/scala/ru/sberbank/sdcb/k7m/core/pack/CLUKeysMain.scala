package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object CLUKeysMain extends BaseMainClass {

  val STAGE_ID_KEY = "stageId"

  override def run(params: Map[String, String], config: Config): Unit = {

    val spark = SparkSession
      .builder
      .enableHiveSupport()
      .appName("clu_keys")
      .getOrCreate

    val StageId = obtainStageId(params)

      val processLogger = new ProcessLogger(spark, config, "clu_keys") // processName- наименование процесса,
      processLogger.logStartProcess()

      val CluKeysRawDF = new CluKeysRawClass(spark,config).DoCluKeysRaw()

      val CluKeysCoreDF = new CluKeysCoreClass(spark,config).DoCluKeysCore()

      val CluKeysPrepareDF = new CluKeysPrepareClass(spark,config).DoCluKeysPrepare()

      val CluKeysActiveDF = new CluKeysActiveClass(spark,config).DoCluKeysActive()

      val CluKeysNewDF = new CluKeysNewClass(spark,config).DoCluKeysNew()

      val CluKeysResultDF = new CluKeysResultClass(spark,config).DoCluKeysResult()

      processLogger.logEndProcess()
  }
  def obtainStageId(params: Map[String, String]): String = {
    if (params.contains(STAGE_ID_KEY)) params(STAGE_ID_KEY) else "all"
  }
}
