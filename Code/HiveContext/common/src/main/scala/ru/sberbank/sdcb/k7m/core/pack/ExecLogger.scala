package ru.sberbank.sdcb.k7m.core.pack

import java.sql.Timestamp
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.time.{LocalDate, LocalDateTime}
import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.ExecStatus.ExecStatus

class ExecLogger(val spark: SparkSession, val logSchemaName: String) {

  val EXEC_TABLE = s"$logSchemaName.${LoggerUtils.CUSTOM_EXEC_STTS_TABLE}"
  val DATE_TIME_FORMAT = new DateTimeFormatterBuilder()
    .appendValueReduced(ChronoField.YEAR_OF_ERA, 2, 2, 2000)
    .appendValue(ChronoField.MONTH_OF_YEAR, 2)
    .appendValue(ChronoField.DAY_OF_MONTH, 2)
    .appendValue(ChronoField.HOUR_OF_DAY, 2)
    .appendValue(ChronoField.MINUTE_OF_HOUR, 2).toFormatter()

  def logStart(execId: String, runDtParam: String): Unit = {
    val id = if (execId == null) LocalDateTime.now().format(DATE_TIME_FORMAT) else execId
    val runDtExtracted =
      if (runDtParam != null) {
        LocalDate.parse(runDtParam).atStartOfDay()
      } else {
        LocalDate.now().minusDays(1L).atStartOfDay()
      }
    val runDt = Timestamp.valueOf(runDtExtracted)
    insertIntoExecLogTable(id, runDt, ExecStatus.RUNNING)
  }

  def updateStatus(execId: String, status: ExecStatus): Unit = {
    val runDt = LoggerUtils.obtainRunDtTimestamp(spark, logSchemaName)
    insertIntoExecLogTable(execId, runDt, status)
  }

  def logFinish(execId: String): Unit = {
    updateStatus(execId, ExecStatus.SUCCESS)
  }

  def logFail(execId: String): Unit = {
    updateStatus(execId, ExecStatus.FAILED)
  }


  def insertIntoExecLogTable(execId: String, runDt: Timestamp, status: ExecStatus): Unit = {
    val tableRow = spark
      .createDataFrame(Seq((
        execId,
        runDt,
        status.toString,
        currentTime())))
      .toDF(
        LoggerUtils.EXEC_ID,
        LoggerUtils.RUN_DT,
        LoggerUtils.STATUS,
        LoggerUtils.INSERT_DT)
    tableRow.write.mode(SaveMode.Append).insertInto(EXEC_TABLE)
  }

  def currentTime(): Timestamp = {
    new Timestamp(new Date().getTime)
  }
}