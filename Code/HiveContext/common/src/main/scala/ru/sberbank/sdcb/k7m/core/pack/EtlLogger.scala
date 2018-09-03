package ru.sberbank.sdcb.k7m.core.pack

import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.SaveMode
import ru.sberbank.sdcb.k7m.core.pack.CustomLogStatus.CustomLogStatus
import ru.sberbank.sdcb.k7m.core.pack.LogStatus.LogStatus
import ru.sberbank.sdcb.k7m.core.pack.RunStatus.RunStatus

/**
  * Логгирование процесса расчета витрин
  */
trait EtlLogger {
  this: EtlJob =>

  /**
    * Конфигурация
    */
  val config: Config

  lazy val execId: String = obtainExecId()

  def obtainExecId(): String = {
    LoggerUtils.obtainExecId(spark, config.aux)
  }

  def logStartProcess(): Unit = {
    insertIntoLogTable(LogStatus.RUNNING)
  }

  def logStart(step: String = dashboardName): Unit = {
    insertIntoRunTable(step, RunStatus.RUNNING, 0L)
  }


  def logInserted(step: String = dashboardName,
                 count: Long = spark.sql(s"select count(*) from $dashboardName").first().getLong(0)): Unit = {
    insertIntoRunTable(step, RunStatus.RUNNING, count)
    if (count == 0) log(step, s"Сохранена витрина $step с количеством записей = 0", CustomLogStatus.WARNING)
  }

  def logEnd(step: String = dashboardName, status: RunStatus = RunStatus.SUCCESS): Unit = {
    insertIntoRunTable(step, status, 0L)
  }

  def logEndProcess(): Unit = {
    insertIntoLogTable(LogStatus.SUCCESS)
  }

  def log(objectId: String = "", runMessage: String, status: CustomLogStatus = CustomLogStatus.INFO): Unit = {
    insertIntoCustomLogTable(objectId, runMessage, status)
  }

  def currentTime(): Timestamp = {
    new Timestamp(new Date().getTime)
  }

  def runTable: String = {
    s"${config.aux}.${LoggerUtils.RUN_TABLE}"
  }

  def logTable: String = {
    s"${config.aux}.${LoggerUtils.LOG_TABLE}"
  }

  def customLogTable: String = {
    s"${config.aux}.${LoggerUtils.CUSTOM_LOG_TABLE}"
  }

  def insertIntoRunTable(step: String = dashboardName, status: RunStatus, count: Long): Unit = {
    val time = currentTime()
    val runTableRow = spark
      .createDataFrame(Seq((
        execId,
        processName,
        step,
        status.toString,
        time,
        count)))
      .toDF(
        LoggerUtils.EXEC_ID,
        LoggerUtils.PROC_CD,
        LoggerUtils.STEP_CD,
        LoggerUtils.STATUS,
        LoggerUtils.RUN_TS,
        LoggerUtils.DATA_COUNT)
    runTableRow.write.mode(SaveMode.Append).insertInto(runTable)
  }

  def insertIntoLogTable(status: LogStatus): Unit = {
    val time = currentTime()
    val logTableRow = spark
      .createDataFrame(Seq((
        execId,
        processName,
        status.toString,
        time)))
      .toDF(
        LoggerUtils.EXEC_ID,
        LoggerUtils.PROC_CD,
        LoggerUtils.STATUS,
        LoggerUtils.RUN_TS)
    logTableRow.write.mode(SaveMode.Append).insertInto(logTable)
  }

  def insertIntoCustomLogTable(objectId: String, runMessage: String, status: CustomLogStatus): Unit = {
    val time = currentTime()
    val customlogTableRow = spark
      .createDataFrame(Seq((
        execId,
        processName,
        objectId,
        runMessage,
        status.toString,
        time)))
      .toDF(
        LoggerUtils.EXEC_ID,
        LoggerUtils.PROC_CD,
        LoggerUtils.OBJ_ID,
        LoggerUtils.RUN_MESSAGE,
        LoggerUtils.STATUS,
        LoggerUtils.RUN_TS)
    customlogTableRow.write.mode(SaveMode.Append).insertInto(customLogTable)
  }

}