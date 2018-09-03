package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

trait EtlJob {

  /**
    * Экземлпляр SparkSession
    */
  def spark: SparkSession

  /**
    * Наименование витрины в формате: "schema.tableName"
    */
  def dashboardName: String

  /**
    * Наименование процедуры
    */
  def processName: String = dashboardName
}
