package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CrmPositionClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_K7M_CRM_POSITIONIN = s"${Stg0Schema}.crm_s_postn"
  val Node2t_team_k7m_aux_d_K7M_CRM_POSITIONIN = s"${Stg0Schema}.crm_s_party"
  val Node3t_team_k7m_aux_d_K7M_CRM_POSITIONIN = s"${Stg0Schema}.crm_s_org_ext"
  val Nodet_team_k7m_aux_d_K7M_CRM_POSITIONOUT = s"${DevSchema}.K7M_CRM_POSITION"
  val dashboardPath = s"${config.auxPath}K7M_CRM_POSITION"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CRM_POSITIONOUT //витрина
  override def processName: String = SparkMainClass.applicationName

  def DoCrmPosition() {

    Logger.getLogger("org").setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
          p.row_ID                as ID                   --ID--
          ,p.pr_emp_id         as main_user_id     --ID основного сотрудника--
          ,p.name              as name             --Наименование позиции--
          ,p.OU_ID             as adm_org_id       --Орг. Подразделение--
          ,pt.par_party_id     as parent_id        --Родительская позиция--
          ,org.x_osb_div_id    as terr_org_id      --Терр. Подразделение--
          ,p.BU_ID             as terr_tb_id       --Терр. Подразделение ЦА/ТБ--
      from $Node1t_team_k7m_aux_d_K7M_CRM_POSITIONIN p
      left join $Node2t_team_k7m_aux_d_K7M_CRM_POSITIONIN pt
        on pt.row_id = p.row_id
      left join $Node3t_team_k7m_aux_d_K7M_CRM_POSITIONIN org
        on p.OU_ID =org.PAR_ROW_ID"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_POSITIONOUT")

    logInserted()
    logEnd()
  }
}
