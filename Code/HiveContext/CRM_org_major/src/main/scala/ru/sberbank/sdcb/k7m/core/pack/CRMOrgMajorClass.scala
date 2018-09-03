package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CRMOrgMajorClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa
  
  val Node1t_team_k7m_aux_d_k7m_crm_org_majorIN = s"${DevSchema}.k7m_crm_org_mj"
  val Nodet_team_k7m_aux_d_k7m_crm_org_majorOUT = s"${DevSchema}.k7m_crm_org_major"
  val dashboardPath = s"${config.auxPath}k7m_crm_org_major"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_crm_org_majorOUT //витрина
  override def processName: String = "CRM_ORG_MAJOR"

  def DoOrgMajor() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_crm_org_majorOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
       id,
       inn,
       full_name
  from $Node1t_team_k7m_aux_d_k7m_crm_org_majorIN
 where rn_inn = 1"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_crm_org_majorOUT")

    logInserted()
    logEnd()
  }


}
