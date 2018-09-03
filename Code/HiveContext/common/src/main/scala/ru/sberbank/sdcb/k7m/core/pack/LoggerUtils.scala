package ru.sberbank.sdcb.k7m.core.pack

import java.sql.Timestamp

import org.apache.spark.sql.{SaveMode, SparkSession}

object LoggerUtils {

  val RUN_TABLE = "ETL_BKMART_RUN"
  val LOG_TABLE = "ETL_BKMART_LOG"
  val CUSTOM_LOG_TABLE = "ETL_CUSTOM_LOG"
  val CUSTOM_EXEC_STTS_TABLE = "ETL_EXEC_STTS"

  val EXEC_ID = "EXEC_ID"
  val PROC_CD = "PROC_CD"
  val STEP_CD = "STEP_CD"
  val STATUS = "STATUS"
  val RUN_TS = "RUN_TS"
  val DATA_COUNT = "DATA_COUNT"
  val OBJ_ID = "OBJ_ID"
  val RUN_MESSAGE = "RUN_MESSAGE"
  val RUN_DT = "RUN_DT"
  val INSERT_DT = "INSERT_DT"

  val RUNNING_STATUS = "RUNNING"
  val FAILED_STATUS = "FAILED"
  val SUCCESS_STATUS = "SUCCESS"
  val BACKUPED_STATUS = "BACKUPED"
  val ABORTED_STATUS = "ABORTED"
  val INFO_STATUS = "INFO"
  val ERROR_STATUS = "ERROR"
  val WARNING_STATUS = "WARNING"


  def createTables(spark: SparkSession, logSchemaName: String): Unit = {
    createRunTable(spark, logSchemaName)
    createLogTable(spark, logSchemaName)
    createCustomLogTable(spark, logSchemaName)
    createExecLogTable(spark, logSchemaName)
  }

  def createRunTable(spark: SparkSession, logSchemaName: String): Unit = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $logSchemaName.$RUN_TABLE
        (
            $EXEC_ID STRING, -- порядковый ID запуска расчёта,
            $PROC_CD STRING, -- процедура
            $STEP_CD STRING, -- наименование шага расчета
            $STATUS STRING, -- статус
            $RUN_TS TIMESTAMP, -- дата/время внесения записи
            $DATA_COUNT BIGINT -- характеристика объёма обработанной информации
        )""")
  }

  def createLogTable(spark: SparkSession, logSchemaName: String): Unit = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $logSchemaName.$LOG_TABLE
         (
             $EXEC_ID STRING, -- порядковый ID запуска расчёта,
             $PROC_CD STRING, -- процедура
             $STATUS STRING, -- статус
             $RUN_TS TIMESTAMP -- дата/время начала расчёта
         )""")
  }

  def createCustomLogTable(spark: SparkSession, logSchemaName: String): Unit = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $logSchemaName.$CUSTOM_LOG_TABLE
         (
             $EXEC_ID STRING, -- порядковый ID запуска расчёта,
             $PROC_CD STRING, -- процедура
             $OBJ_ID STRING, -- идентификатор объекта
             $RUN_MESSAGE STRING, -- сообщение
             $STATUS STRING, -- статус
             $RUN_TS TIMESTAMP -- дата/время
         )""")
  }

  def createExecLogTable(spark: SparkSession, logSchemaName: String): Unit = {
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $logSchemaName.$CUSTOM_EXEC_STTS_TABLE
         (
             $EXEC_ID STRING, -- порядковый ID запуска расчёта
             $RUN_DT TIMESTAMP, -- бизнес-дата расчёта
             $STATUS STRING, -- статус
             $INSERT_DT TIMESTAMP -- дата вставки записи в таблицу
         )""")
  }

  def obtainExecId(spark: SparkSession, logSchemaName: String): String = {
    val count = activeRecordsCount(spark, logSchemaName)
    if (count != 1L) {
      throw new IllegalStateException(s"Неверное состояние таблицы ETl_EXEC_STTS, кол-во активных запусков: $count")
    }
    spark
      .sql(s"select temp.${LoggerUtils.EXEC_ID} from (select ${LoggerUtils.EXEC_ID}, ${LoggerUtils.INSERT_DT}, ${LoggerUtils.STATUS}, ${LoggerUtils.RUN_DT}, " +
        s"row_number() over (partition by ${LoggerUtils.EXEC_ID} order by ${LoggerUtils.INSERT_DT} desc) as rn " +
        s"from $logSchemaName.$CUSTOM_EXEC_STTS_TABLE) temp " +
        s"where temp.rn=1 and (temp.${LoggerUtils.STATUS} = '${ExecStatus.RUNNING}' or temp.${LoggerUtils.STATUS} = '${ExecStatus.FAILED}')")
      .first()
      .getString(0)
  }

  /**
    * Возвращает параметр даты в формате timestamp (дата с временем)
    * например 2018-04-27 00:00:00
    */
  def obtainRunDt(spark: SparkSession, logSchemaName: String): String = {
    spark
      .sql(s"select cast(temp.${LoggerUtils.RUN_DT} as String) from (select ${LoggerUtils.EXEC_ID}, ${LoggerUtils.INSERT_DT}, ${LoggerUtils.STATUS}, ${LoggerUtils.RUN_DT}," +
        s"row_number() over (partition by ${LoggerUtils.EXEC_ID} order by ${LoggerUtils.INSERT_DT} desc) as rn " +
        s"from $logSchemaName.$CUSTOM_EXEC_STTS_TABLE) temp " +
        s"where temp.rn=1 and (temp.${LoggerUtils.STATUS} = '${ExecStatus.RUNNING}' or temp.${LoggerUtils.STATUS} = '${ExecStatus.FAILED}')")
      .first()
      .getString(0)
  }

  /**
    * Возвращает параметр даты в формате даты
    * например 2018-04-27
    */
  def obtainRunDtWithoutTime(spark: SparkSession, logSchemaName: String): String = {
    spark
      .sql(s"select cast(temp.${LoggerUtils.RUN_DT} as String) from (select ${LoggerUtils.EXEC_ID}, ${LoggerUtils.INSERT_DT}, ${LoggerUtils.STATUS}, ${LoggerUtils.RUN_DT}," +
        s"row_number() over (partition by ${LoggerUtils.EXEC_ID} order by ${LoggerUtils.INSERT_DT} desc) as rn " +
        s"from $logSchemaName.$CUSTOM_EXEC_STTS_TABLE) temp " +
        s"where temp.rn=1 and (temp.${LoggerUtils.STATUS} = '${ExecStatus.RUNNING}' or temp.${LoggerUtils.STATUS} = '${ExecStatus.FAILED}')")
      .first()
      .getString(0).substring(0, 10)
  }


  def obtainRunDtTimestamp(spark: SparkSession, logSchemaName: String): Timestamp = {
    spark
      .sql(s"select temp.${LoggerUtils.RUN_DT} from (select ${LoggerUtils.EXEC_ID}, ${LoggerUtils.INSERT_DT}, ${LoggerUtils.STATUS}, ${LoggerUtils.RUN_DT}, " +
        s"row_number() over (partition by ${LoggerUtils.EXEC_ID} order by ${LoggerUtils.INSERT_DT} desc) as rn " +
        s"from $logSchemaName.$CUSTOM_EXEC_STTS_TABLE) temp " +
        s"where temp.rn=1 and (temp.${LoggerUtils.STATUS} = '${ExecStatus.RUNNING}' or temp.${LoggerUtils.STATUS} = '${ExecStatus.FAILED}')")
      .first()
      .getTimestamp(0)
  }


  def checkCanStart(spark: SparkSession, logSchemaName: String): Boolean = {
    val count = activeRecordsCount(spark, logSchemaName)
    count == 0L
  }

  def activeRecordsCount(spark: SparkSession, logSchemaName: String): Long = {
    spark
      .sql(s"select count(*) from (select ${LoggerUtils.EXEC_ID}, ${LoggerUtils.INSERT_DT}, ${LoggerUtils.STATUS}, ${LoggerUtils.RUN_DT}, " +
        s"row_number() over (partition by ${LoggerUtils.EXEC_ID} order by ${LoggerUtils.INSERT_DT} desc) as rn " +
        s"from $logSchemaName.$CUSTOM_EXEC_STTS_TABLE) temp " +
        s"where temp.rn=1 and (temp.${LoggerUtils.STATUS} = '${ExecStatus.RUNNING}' or temp.${LoggerUtils.STATUS} = '${ExecStatus.FAILED}')")
      .first().getLong(0)
  }

  def mergeFiles(spark: SparkSession, logSchemaName: String, path: String): Unit = {
    mergeTableFiles(spark, logSchemaName, path, RUN_TABLE)
    mergeTableFiles(spark, logSchemaName, path, CUSTOM_LOG_TABLE)
    mergeTableFiles(spark, logSchemaName, path, LOG_TABLE)
  }

  def mergeTableFiles(spark: SparkSession, logSchemaName: String, path: String, tableName: String): Unit = {
    val tableNameLowered = tableName.toLowerCase
    val tempTable = tableNameLowered + "_temp"
    spark.sql(s"select * from $logSchemaName.$tableNameLowered")
      .write
      .mode(SaveMode.Overwrite)
      .option("path", s"$path$tempTable")
      .saveAsTable(s"$logSchemaName.$tempTable")
    spark.sql(s"select * from $logSchemaName.$tempTable")
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("path", s"$path$tableNameLowered")
      .saveAsTable(s"$logSchemaName.$tableNameLowered")
    spark.sql(s"drop table $logSchemaName.$tempTable")
  }
}

object ExecStatus extends Enumeration {
  type ExecStatus = Value
  val RUNNING = Value(LoggerUtils.RUNNING_STATUS)
  val SUCCESS = Value(LoggerUtils.SUCCESS_STATUS)
  val FAILED = Value(LoggerUtils.FAILED_STATUS)
  val BACKUPED = Value(LoggerUtils.BACKUPED_STATUS)
  val ABORTED = Value(LoggerUtils.ABORTED_STATUS)
}

object LogStatus extends Enumeration {
  type LogStatus = Value
  val RUNNING = Value(LoggerUtils.RUNNING_STATUS)
  val SUCCESS = Value(LoggerUtils.SUCCESS_STATUS)
  val FAILED = Value(LoggerUtils.FAILED_STATUS)
}

object RunStatus extends Enumeration {
  type RunStatus = Value
  val RUNNING = Value(LoggerUtils.RUNNING_STATUS)
  val SUCCESS = Value(LoggerUtils.SUCCESS_STATUS)
  val FAILED = Value(LoggerUtils.FAILED_STATUS)
}

object CustomLogStatus extends Enumeration {
  type CustomLogStatus = Value
  val INFO = Value(LoggerUtils.INFO_STATUS)
  val ERROR = Value(LoggerUtils.ERROR_STATUS)
  val WARNING = Value(LoggerUtils.WARNING_STATUS)
}
