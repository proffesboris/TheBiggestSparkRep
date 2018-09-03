package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClientCrmKredClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Node1t_team_k7m_aux_d_basis_client_crm_kred_cluIN = s"${config.stg}.crm_s_org_ext_x"
  val Node2t_team_k7m_aux_d_basis_client_crm_kred_cluIN = s"${config.stg}.crm_s_optyprd_org"
  val Node3t_team_k7m_aux_d_basis_client_crm_kred_cluIN = s"${config.stg}.crm_s_revn"
  val Node4t_team_k7m_aux_d_basis_client_crm_kred_cluIN = s"${config.stg}.crm_s_opty"
  val Node5t_team_k7m_aux_d_basis_client_crm_kred_cluIN = s"${config.stg}.crm_s_opty_prod_fnx"
  val Nodet_team_k7m_aux_d_basis_client_crm_kred_cluOUT = s"${config.aux}.client_crm_kred_clu"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_crm_kred_cluOUT //витрина
  override def processName: String = "CLU"

  val dashboardPath = s"${config.auxPath}basis_client_crm_kred_clu"

  def DoClientCrmKred() {

    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_crm_kred_cluOUT).setLevel(Level.WARN)



    val createHiveTableStage1 = spark.sql(
      s"""select distinct
      c.par_row_id as org_id,                     /*уникальный id в internal_crm_cb_siebel.s_org_ext*/
      c.sbrf_inn as org_inn,                             /* ИНН */
      o.opty_prd_id,                            /* id в internal_crm_cb_siebel.S_OPTYPRD_ORG(КК, ОПК, СМО - Продукты/Кредитные условия - Участники)*/
      o.role_cd,                                /* тип участника : заемщик, поручитель и пр. */
      o.x_part_type,                            /* вид участника : информация СРМ */
      r.row_id as rev_row_id,                   /* ID кредитного продукта*/
      r.REVN_STAT_CD,                           /* статус кредитного продукта 'Действующий','В процессе изменения' */
      prod_fnx.x_credit_mode                    /*Инструмент кредитования заемщика для кредитных сделок*/
  from $Node1t_team_k7m_aux_d_basis_client_crm_kred_cluIN c
  join $Node2t_team_k7m_aux_d_basis_client_crm_kred_cluIN o on o.OU_ID = c.par_row_id and o.status_cd is null
  join $Node3t_team_k7m_aux_d_basis_client_crm_kred_cluIN r on o.OPTY_PRD_ID = r.row_id and r.REVN_STAT_CD in ('Действующий','В процессе изменения')
  join $Node4t_team_k7m_aux_d_basis_client_crm_kred_cluIN opt on r.opty_id = opt.row_id
  left join $Node5t_team_k7m_aux_d_basis_client_crm_kred_cluIN prod_fnx on prod_fnx.par_row_id=r.row_id
"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_crm_kred_cluOUT")

    logInserted()

  }


}
