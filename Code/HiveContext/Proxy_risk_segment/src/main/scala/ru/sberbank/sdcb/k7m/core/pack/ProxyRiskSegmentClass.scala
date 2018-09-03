package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class ProxyRiskSegmentClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_proxy_risk_segment_inIN = s"${config.pa}.CLU"
  val Nodet_team_k7m_aux_d_proxy_risk_segment_inOUT = s"${config.pa}.proxy_risk_segment_in"


  override val dashboardName: String = Nodet_team_k7m_aux_d_proxy_risk_segment_inOUT //витрина
  val dashboardPath = s"${config.paPath}proxy_risk_segment_in"

  override def processName: String = "ProxyRiskSegment"

  def DoProxyRiskSegment() {
    Logger.getLogger(Nodet_team_k7m_aux_d_proxy_risk_segment_inOUT).setLevel(Level.WARN)

    logStart()

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
         select
         		U7M_ID,
         		CRM_ID,
         		EKS_ID,
         		INN,
         		INDUSTRY,
         		OKK,
         		OKVED_CRM_CD,
         		OKVED,
         		REG_DATE,
         		RSEGMENT
           from
         		$Node1t_team_k7m_aux_d_proxy_risk_segment_inIN
          where
         		coalesce(CRM_ID, '') <> '' or
         		coalesce(INN, '') <> ''
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_proxy_risk_segment_inOUT")

    logInserted()
    logEnd()
  }
}
