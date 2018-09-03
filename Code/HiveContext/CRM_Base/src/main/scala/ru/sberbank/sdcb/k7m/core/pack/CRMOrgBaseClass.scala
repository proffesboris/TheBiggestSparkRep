package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CRMOrgBaseClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa
  val Node1t_team_k7m_aux_d_K7M_CRM_org_baseIN = s"${Stg0Schema}.crm_s_org_ext"
  val Node2t_team_k7m_aux_d_K7M_CRM_org_baseIN = s"${Stg0Schema}.crm_s_org_ext_fnx"
  val Node3t_team_k7m_aux_d_K7M_CRM_org_baseIN = s"${Stg0Schema}.crm_s_org_ext_x"
  val Nodet_team_k7m_aux_d_K7M_CRM_org_baseOUT = s"${DevSchema}.K7M_CRM_org_base"
  val dashboardPath = s"${config.auxPath}K7M_CRM_org_base"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CRM_org_baseOUT //витрина
  override def processName: String = "CRM_BASE"

  def DoCRMOrgBase() {

    Logger.getLogger(Nodet_team_k7m_aux_d_K7M_CRM_org_baseOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      soe.row_id as id,
      soe.x_div_head_id as main_position_id,
      soe.par_divn_id as parent_id,
      soex.x_business_direction as business_area,
      soe.created as created_dt,
      soef.vndr_cmpny_uid as code,
      soex.attrib_34 as short_name,
      soe.name,
      soe.x_hier_lvl as lvl
  from $Node1t_team_k7m_aux_d_K7M_CRM_org_baseIN soe
  left join $Node2t_team_k7m_aux_d_K7M_CRM_org_baseIN soef
    on soe.row_id = soef.par_row_id
  left join $Node3t_team_k7m_aux_d_K7M_CRM_org_baseIN soex
    on soe.row_id = soex.par_row_id
  where soe.int_org_flg='Y'"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_org_baseOUT")

    logInserted()
    logEnd()
  }
}
