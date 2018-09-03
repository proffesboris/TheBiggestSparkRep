package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object IntegrumMainClass extends BaseMainClass {

  override def run(params: Map[String, String], config: Config): Unit = {
    val spark = SparkSession
      .builder
      .appName("IntegrumBase")
      .getOrCreate

    val processLogger = new ProcessLogger(spark, config, "IntBase") // processName- наименование процесса,
    processLogger.logStartProcess()

    val dashboardName = s"${config.aux}.int_fin_stmt_rsbu"
    val integrumDashboard = new IntegrumDashboard(spark, dashboardName, config)
    val dateStr = LoggerUtils.obtainRunDt(spark, config.aux)
    integrumDashboard.doCreateIntFinStmtRsbu(dateStr)

    processLogger.logEndProcess()
  }

}
