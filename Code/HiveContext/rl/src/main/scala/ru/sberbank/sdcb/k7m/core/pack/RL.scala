package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class RL(override val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  override val processName = "RL"
  var dashboardName = "RL"

  def run(): Unit = {
    logStart()

    if (spark.catalog.tableExists(config.stg, "RL_CONCLUSION") && !spark.table(s"${config.stg}.RL_CONCLUSION").rdd.isEmpty()) {

      val rlc =
        spark.table(s"${config.stg}.RL_CONCLUSION")
          .select(col("request_id"), explode(col("legalEntities")) as "legalEntities")
          .select("request_id", "legalEntities.id", "legalEntities.result")
          .groupBy("id")
          .agg(
            first("request_id") as "request_id",
            first("result") as "result"
          )

      dashboardName = s"${config.aux}.RL_CLU"

      spark.table(s"${config.pa}.CLU").as("clu")
        .join(rlc as "rlc", col("clu.crm_id") === col("rlc.id"), "left")
        .withColumn("OFFLINE_RU_ID", col("rlc.request_id"))
        .withColumn("OFFLINE_RU_MARK", col("rlc.result"))
        .select("clu.*", "OFFLINE_RU_ID", "OFFLINE_RU_MARK")
        .write
        .format("parquet")
        .option("path", s"${config.auxPath}RL_CLU")
        .mode(SaveMode.Overwrite)
        .saveAsTable(s"${config.aux}.RL_CLU")

      logInserted()

      dashboardName = s"${config.pa}.CLU"

      spark.table(s"${config.aux}.RL_CLU")
        .write
        .format("parquet")
        .option("path", s"${config.paPath}CLU")
        .mode(SaveMode.Overwrite)
        .saveAsTable(s"${config.pa}.CLU")

      logInserted()

    }

    logEnd()
  }

}