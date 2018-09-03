package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession


object SparkMain extends BaseMainClass {

  override def run(params: Map[String, String], config: Config): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("GroupInfluenceIn")
      .getOrCreate()

    val processLogger = new ProcessLogger(spark, config, "GroupInfluenceIn") // processName- наименование процесса,
    processLogger.logStartProcess()

    new GroupInfluenceIn(spark, config).run()

    processLogger.logEndProcess()
  }
}
