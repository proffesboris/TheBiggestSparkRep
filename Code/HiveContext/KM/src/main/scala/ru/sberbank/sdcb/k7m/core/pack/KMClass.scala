package ru.sberbank.sdcb.k7m.core.pack

/**
  * Created by sbt-medvedev-ba on 08.02.2018.
  */


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class KMClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_K7M_KMIN = s"${DevSchema}.K7M_crm_org"
  val Node2t_team_k7m_aux_d_K7M_KMIN = s"${DevSchema}.K7M_CRM_CUST_TEAM"
  val Node3t_team_k7m_aux_d_K7M_KMIN = s"${DevSchema}.K7M_CRM_POSITION"
  val Node4t_team_k7m_aux_d_K7M_KMIN = s"${DevSchema}.K7M_CRM_USER"
  val Node5t_team_k7m_aux_d_K7M_KMIN = s"${DevSchema}.K7M_crm_org_struct"
  val Nodet_team_k7m_aux_d_K7M_KMOUT = s"${DevSchema}.K7M_KM"
  val dashboardPath = s"${config.auxPath}K7M_KM"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_KMOUT //витрина
  override def processName: String = SparkMainClass.applicationName

  def DoKM() {

    Logger.getLogger("org").setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
     row_number() over (order by crm_org_id) loc_id, t.*
    from
    (
    select distinct
        c.id  as crm_org_id
      , u.id  as KM_id
      , u.F_NAME
      , u.S_NAME
      , u.L_NAME
      , u.M_PHONE
      , u.W_PHONE
      , u.EMAIL
      , u.b_date
      ,o.FUNC_DIVISION
      ,o.TER_DIVISION
      ,tb.TER_DIVISION ca_tb
  from $Node1t_team_k7m_aux_d_K7M_KMIN c
  join $Node2t_team_k7m_aux_d_K7M_KMIN t
    on c.id = t.cust_id
  join $Node3t_team_k7m_aux_d_K7M_KMIN p
    on t.position_id = p.id
  join $Node4t_team_k7m_aux_d_K7M_KMIN u
    on p.main_user_id = u.id
  left join $Node5t_team_k7m_aux_d_K7M_KMIN o
    on o.id = p.terr_org_id
  left join $Node5t_team_k7m_aux_d_K7M_KMIN tb
    on tb.id = p.terr_tb_id
    ) t """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_KMOUT")

    logInserted()
    logEnd()
  }


}
