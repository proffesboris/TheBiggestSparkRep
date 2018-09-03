package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CrmOrgStructClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_K7M_crm_org_structIN = s"${Stg0Schema}.crm_s_org_ext"
  val Node2t_team_k7m_aux_d_K7M_crm_org_structIN = s"${Stg0Schema}.crm_s_org_ext_x"
  val Nodet_team_k7m_aux_d_K7M_crm_org_structOUT = s"${DevSchema}.K7M_crm_org_struct"
  val dashboardPath = s"${config.auxPath}K7M_crm_org_struct"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_crm_org_structOUT //витрина
  override def processName: String = SparkMainClass.applicationName

  def DoCrmOrgStruct() {

    Logger.getLogger("org").setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      org.row_id as id                                 --ID подразделения--
      ,org_x.x_business_direction as FUNC_DIVISION      --Бизнес-направление--
      ,org.name as TER_DIVISION                         --Наименование--
      ,org.x_hier_lvl as lvl                            --Уровень подразделения--
  from $Node1t_team_k7m_aux_d_K7M_crm_org_structIN org
  left join $Node2t_team_k7m_aux_d_K7M_crm_org_structIN org_x
    on org.row_id = org_x.PAR_ROW_ID
 where org.INT_ORG_FLG='Y'
        """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_crm_org_structOUT")

    logInserted()
    logEnd()
  }
}
