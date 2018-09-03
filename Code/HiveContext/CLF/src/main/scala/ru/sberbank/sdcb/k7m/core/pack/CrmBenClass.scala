package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CrmBenClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_K7M_CRM_BENIN = s"${DevSchema}.k7m_crm_org"
  val Node2t_team_k7m_aux_d_K7M_CRM_BENIN = s"${Stg0Schema}.crm_cx_party_benef"
  val Node3t_team_k7m_aux_d_K7M_CRM_BENIN = s"${Stg0Schema}.crm_S_CONTACT"
  val Node4t_team_k7m_aux_d_K7M_CRM_BENIN = s"${DevSchema}.K7M_CRM_ADDR"
  val Node5t_team_k7m_aux_d_K7M_CRM_BENIN = s"${DevSchema}.K7M_CRM_reg_doc"
   val Nodet_team_k7m_aux_d_K7M_CRM_BENOUT = s"${DevSchema}.K7M_CRM_BEN"
  val dashboardPath = s"${config.auxPath}K7M_CRM_BEN"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CRM_BENOUT //витрина
  override def processName: String = "CLF"

  def DoCrmBen() {

    Logger.getLogger("org").setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""select
               row_number() over (order by cust1.id) loc_id  --Добавить уникальный loc_id = BE_0123456789
             , cust1.id
             , cust1.inn
             , cust1.kpp
             , cust1.full_name
             , cust1.legal_form_cd
              -----юл+ип --
             , cust2.id     as benorg_id--
             , cust2.inn    as benorg_inn
             , cust2.kpp    as benorg_kpp
             , cust2.full_name as benorg_full_name
             , cust2.legal_form_cd    as benorg_opf
             , addrj.jur_addr  as org_jur_addr
             , addrj.fact_addr as org_fact_addr
             , con.row_id   as fz_ben
             , con.fst_name
             , con.mid_name
             , con.last_name
             , con.birth_dt
             , con.sex_mf
             , con.citizenship_cd
             , con.home_ph_num
             , con.work_ph_num
             , con.cell_ph_num
             , con.email_addr
             , addrc.jur_addr as fz_jur_addr
             , addrc.fact_addr as fz_fact_addr
             , rd.type    ---- тип дока
             , rd.series    --  Серия
             , rd.doc_number    ---Место рождения
             , rd.registrator   ----Кем выдан
             , rd.issue_dt   ---Дата выдачи
             , rd.birth_place   ---Место рождения
             , rd.reg_code  ---Код подразделения
             , rd.end_date ----Планируемая дата окончания срока действия
             , ben.shares           as quantity
             , 1                    as link_prob
             , 'BEN_CRM'            as crit
from $Node1t_team_k7m_aux_d_K7M_CRM_BENIN               cust1
         join $Node2t_team_k7m_aux_d_K7M_CRM_BENIN      ben               on  ben.account_id  = cust1.id
         left join $Node3t_team_k7m_aux_d_K7M_CRM_BENIN      con          on  ben.benef_id    = con.row_id
         left join $Node4t_team_k7m_aux_d_K7M_CRM_BENIN         addrc             on  addrc.contact_id  = con.row_id
         left join $Node1t_team_k7m_aux_d_K7M_CRM_BENIN          cust2             on  ben.benef_id    = cust2.id
         left join $Node4t_team_k7m_aux_d_K7M_CRM_BENIN         addrj             on  addrj.accnt_id  = cust2.id
         left join $Node5t_team_k7m_aux_d_K7M_CRM_BENIN      rd                on  con.row_id      = rd.contact_id
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_BENOUT")

    logInserted()
    logEnd()
  }
}

