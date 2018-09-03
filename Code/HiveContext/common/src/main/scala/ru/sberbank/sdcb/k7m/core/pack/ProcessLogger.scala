package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

/**
  * Логгер процесса
  * @param spark экземляр сессии спарка
  * @param config конфигурация
  * @param processName наименование процесса
  */
class ProcessLogger(val spark: SparkSession, val config: Config, override val processName: String) extends EtlLogger with EtlJob {

  val dashboardName = ""

}
