package ru.sberbank.sdcb.k7m.core.pack

import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.LogStatus.LogStatus
import ru.sberbank.sdcb.k7m.core.pack.RunStatus.RunStatus

/**
  * Логгирование процесса расчета витрин
  */
trait EtlIntegrLogger {
  this: EtlJob =>

  /**
    * Схема, содердащая таблички логгирования
    */
  val logSchemaName: String

  /**
    * Экземпляр расчёта
    */
  def execId: String


  def logIntegrStart(step: String = dashboardName,
                        runStatus: RunStatus = RunStatus.SUCCESS,
                        logStatus: LogStatus = LogStatus.RUNNING): Unit = {
    insertIntoIntegrTable(logStatus.toString, EtlIntegrLogger.SUCCESS_CODE, 0L)
  }


  def logIntegrSaved(count: Long,
                  runMessage: String = s"$dashboardName was copied to TFS successfully",
                  errorCode: Long = EtlIntegrLogger.SUCCESS_CODE): Unit = {
    insertIntoIntegrTable(runMessage, errorCode, count)
  }

  def logIntegrInserted(step: String = dashboardName,
                  count: Long = spark.sql(s"select count(*) from $dashboardName").first().getLong(0)): Unit = {
    insertIntoIntegrTable("inserted", EtlIntegrLogger.SUCCESS_CODE, count)
  }

  def logIntegrEnd(step: String = dashboardName,
                      runStatus: RunStatus = RunStatus.SUCCESS,
                      logStatus: LogStatus = LogStatus.SUCCESS): Unit = {
    insertIntoIntegrTable(logStatus.toString, EtlIntegrLogger.SUCCESS_CODE, 0L)
  }

  def integrTable: String = {
    s"$logSchemaName.${EtlIntegrLogger.INTEGR_TABLE}"
  }

  def currentIntegrTime(): Timestamp = {
    new Timestamp(new Date().getTime)
  }

  def insertIntoIntegrTable(runMessage: String, errorCode: Long, count: Long): Unit = {
    val time = currentIntegrTime()
    val runTableRow = spark
      .createDataFrame(Seq((
        EtlIntegrLogger.DMK7M,
        EtlIntegrLogger.K7MALL,
        execId,
        time,
        errorCode,
        dashboardName,
        runMessage,
        count)))
      .toDF(
        EtlIntegrLogger.DMMARTCD,
        EtlIntegrLogger.SCENARIOCD,
        EtlIntegrLogger.EXEC_ID,
        EtlIntegrLogger.RUN_TS,
        EtlIntegrLogger.ERROR,
        EtlIntegrLogger.PROC,
        EtlIntegrLogger.RUN_MSG,
        EtlIntegrLogger.DATA_COUNT)
    runTableRow.write.mode(SaveMode.Append).insertInto(integrTable)
  }

}

object EtlIntegrLogger {
  val INTEGR_TABLE = "ETL_BKMART_INTEGR"

  val DMMARTCD = "DMMARTCD"
  val SCENARIOCD = "SCENARIOCD"
  val EXEC_ID = "EXEC_ID"
  val RUN_TS = "RUN_TS"
  val ERROR = "ERROR"
  val PROC = "PROC"
  val RUN_MSG = "RUN_MSG"
  val DATA_COUNT = "DATA_COUNT"

  val SUCCESS_CODE = 0L

  val DMK7M = "DMK7M"
  val K7MALL = "K7MALL"

  val START_MESSAGE = "START"
  val DELETED_MESSAGE = "DELETED"
  val INSERTED_MESSAGE = "INSERTED"
  val END_MESSAGE = "END"

  def createTables(spark: SparkSession, logSchemaName: String): Unit = {
    createIntegrTable(spark, logSchemaName)
  }

  def createIntegrTable(spark: SparkSession, logSchemaName: String): Unit = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $logSchemaName.$INTEGR_TABLE
        (
            DMMARTCD CHAR(9), -- код витрины,
            SCENARIOCD CHAR(9), -- сценарий расчёта
            EXEC_ID STRING, -- порядковый ID запуска расчёта
            RUN_TS TIMESTAMP, -- дата/время начала расчёта
            ERROR BIGINT, -- результат загрузки: 0 – OK, иначе код ошибки
            PROC VARCHAR(100), -- сообщение
            RUN_MSG VARCHAR(1000), -- сообщение
            DATA_COUNT BIGINT -- характеристика объёма обработанной информации
        )""")
  }

}