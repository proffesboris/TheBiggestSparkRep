package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object EtlMain extends BaseMainClass {

  val STATUS_PARAM = "status"
  val RUN_DT_PARAM = "runDt"
  val EXEC_ID_PARAM = "execId"


  override def run(params: Map[String, String], config: Config): Unit = {

    val statusValue = params(STATUS_PARAM)
    val status = ExecStatus.withName(statusValue)

    val spark = SparkSession
      .builder()
      .appName("EtlRunner")
      .getOrCreate()

    val logSchemaName = config.aux
    val execLogger = new ExecLogger(spark, logSchemaName)

    if (status == ExecStatus.RUNNING) {
      LoggerUtils.createTables(spark, logSchemaName)
      EtlIntegrLogger.createIntegrTable(spark, logSchemaName)
      if (!LoggerUtils.checkCanStart(spark, logSchemaName)) {
        throw new IllegalStateException("В таблице ETL_EXEC_STTS есть поток в статусе RUNNING!")
      }
      val runDtParam = if (params.contains(RUN_DT_PARAM)) params(RUN_DT_PARAM) else null
      val execIdParam = if (params.contains(EXEC_ID_PARAM)) params(EXEC_ID_PARAM) else null
      execLogger.logStart(execIdParam, runDtParam)
    } else {
      val execIdRunning = LoggerUtils.obtainExecId(spark, logSchemaName)
      execLogger.updateStatus(execIdRunning, status)
      if (status == ExecStatus.SUCCESS) {
        LoggerUtils.mergeFiles(spark, logSchemaName, config.auxPath)
      }
    }
  }
}
