package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object SkrOffMain extends BaseMainClass{

  val keyElte: String = s"""'ELTE'"""
  val keyCur: String = s"""'CURRENCY'"""
  val keyPte: String = s"""'PTE'"""
  val keyLgte: String = s"""'LGTE'"""
  val keyDurU: String = s"""'DUR_U'"""
  val keyKDiscount: String = s"""'K_DISCOUNT'"""
  val keyCcfCOffline: String = s"""'CCF_C_OFFLINE'"""
  val keyElpCoef: String = s"""'ELp_COEF'"""
  val keyPDOfflinePrice: String = s"""'PD_OFFLINE_PRICE'"""
  val keyLGDOfflinePrice: String = s"""'LGD_OFFLINE_PRICE'"""
  val keySkrOffline: String = s"""'SKR_OFFLINE'"""


	override def run(params: Map[String, String], config: Config): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SKROff")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", "10000")
      .config("hive.exec.max.dynamic.partitions.pernode","10000")
      .getOrCreate()

    val processLogger = new ProcessLogger(spark, config, "SKROff") // processName- наименование процесса,
    processLogger.logStartProcess()

    new skrOffClass(spark,config).DoSKROff()

    processLogger.logEndProcess()
	}


}
