package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDateTime

import org.apache.spark.sql.SparkSession

class TestLogDashboard(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  override val dashboardName: String = s"${config.aux}.ETL_BKMART_LOG" //витрина
  val logSchemaName: String = config.aux
  val logSchemaPath: String = config.auxPath

  override def processName: String = "TEST"

  def test(): Unit = {
    LoggerUtils.createExecLogTable(spark, logSchemaName)
    LoggerUtils.createTables(spark, logSchemaName) //создание таблиц через if not exists
    val execLogger = new ExecLogger(spark, logSchemaName)
    val id = LocalDateTime.now().toString
    execLogger.logStart(id, null)
    logStart() //логгирование старта
    logInserted() //логгирование вставки строк в витрину через (select count(*)) либо через явное
    log(runMessage = "testInfoMessage")
    log(runMessage = s"execId=$execId")
    log(objectId = "123", runMessage = "testErrorMessage", status = CustomLogStatus.ERROR)
    logEnd() //логгирование окончания выполнения
    execLogger.logFinish(id)
    LoggerUtils.mergeFiles(spark, logSchemaName, logSchemaPath)
  }

}

object TestLogDashboard extends BaseMainClass {
  override def run(params: Map[String, String], config: Config): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TestLogDashboard")
      .getOrCreate()

    val dashboard = new TestLogDashboard(spark, config)
    dashboard.test()
  }
}
