package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

class StubDashboard(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {



  override def dashboardName: String = s"${config.pa}.set"

  override def processName: String = "SET"

  def execute(): Unit = {
    logStart()

    spark.sql(s"""create table if not exists ${config.pa}.set(
		set_id string,
    u7m_b_id string,
    exec_id string,
    okr_id bigint) stored as parquet""")

    spark.sql(s"""create table if not exists ${config.pa}.set_item(
    set_item_id string,
    set_id string,
    7m_id string,
    obj string,
    exec_id string,
    okr_id bigint,
    offline_mmz_mark string,
    concernment string) stored as parquet""")

    logEnd()
  }

}
