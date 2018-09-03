package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class BasisClCrmKredClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux

  //------------------CRM-----------------------------
  val Node1t_team_k7m_aux_d_basis_client_crm_kredIN = s"${DevSchema}.basis_client_crm"
  val Node2t_team_k7m_aux_d_basis_client_crm_kredIN = s"${Stg0Schema}.crm_S_OPTYPRD_ORG"
  val Node3t_team_k7m_aux_d_basis_client_crm_kredIN = s"${Stg0Schema}.crm_S_REVN"
  val Node4t_team_k7m_aux_d_basis_client_crm_kredIN = s"${Stg0Schema}.crm_s_opty"
  val Node5t_team_k7m_aux_d_basis_client_crm_kredIN = s"${Stg0Schema}.crm_cx_agree_opty"
  val Nodet_team_k7m_aux_d_basis_client_crm_kredOUT = s"${DevSchema}.basis_client_crm_kred"
  val dashboardPath = s"${config.auxPath}basis_client_crm_kred"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_crm_kredOUT //витрина
  override def processName: String = "Basis"

  def DoBasisClCrmKred()//(spark: org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_crm_kredOUT).setLevel(Level.WARN)
    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
         select distinct c.org_id,
                         c.org_inn,
                         o.opty_prd_id,
                         o.role_cd,
                         o.x_part_type,
                         r.REVN_STAT_CD,
                         ao.agree_id
           from $Node1t_team_k7m_aux_d_basis_client_crm_kredIN c
           join $Node2t_team_k7m_aux_d_basis_client_crm_kredIN o
             on o.OU_ID = c.org_id
           join $Node3t_team_k7m_aux_d_basis_client_crm_kredIN r
             on o.OPTY_PRD_ID = r.row_id
            and r.REVN_STAT_CD in ('Действующий', 'В процессе изменения')
           join $Node4t_team_k7m_aux_d_basis_client_crm_kredIN opt
             on r.opty_id = opt.row_id
           join $Node5t_team_k7m_aux_d_basis_client_crm_kredIN ao
             on ao.opty_id = opt.row_id /* Договоры по кредитным сделкам */
      """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_crm_kredOUT")

    logInserted()
    logEnd()
  }
}
