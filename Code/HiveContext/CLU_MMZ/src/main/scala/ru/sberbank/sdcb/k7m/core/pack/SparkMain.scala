package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession


object SparkMain extends BaseMainClass {

  override def run(params: Map[String, String], config: Config): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("CLU_MMZ")
      .getOrCreate()

    new CluMmz(spark, config).run()
  }
}
