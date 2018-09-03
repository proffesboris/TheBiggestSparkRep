package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5212aLinkClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_link_for_5212aIN = s"${DevSchema}.k7m_zalog_dispensed"
  val Node2t_team_k7m_aux_d_k7m_link_for_5212aIN = s"${DevSchema}.k7m_payments_for_5212"
  val Node3t_team_k7m_aux_d_k7m_link_for_5212aIN = s"${DevSchema}.k7m_cred_to_acc"
  val Node4t_team_k7m_aux_d_k7m_link_for_5212aIN = s"${Stg0Schema}.eks_Z_CLIENT"
  val Node5t_team_k7m_aux_d_k7m_link_for_5212aIN = s"${DevSchema}.basis_client"
  val Nodet_team_k7m_aux_d_k7m_link_for_5212aOUT = s"${DevSchema}.k7m_link_for_5212a"
  val dashboardPath = s"${config.auxPath}k7m_link_for_5212a"



  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_link_for_5212aOUT //витрина
  override def processName: String = "EL"

  def DoEL5212aLink() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_link_for_5212aOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  cred_id,
  acc_id,
  issue_dt,
  payment_amt,
  cred_class_id,
  cred_inn,
  zalog_class_id,
  case when zalog_inn <> cred_inn  or  zalog_inn is null or zalog_class_id <>'${SparkMainClass.excludeClassId}' then  zalog_inn else '' end zalog_inn
    from (
      select distinct
        p.cred_id,
      ca.acc_id,
      p.issue_dt,
      p.payment_amt,
      cc.class_id cred_class_id,
      cc.c_inn cred_inn,
      cz.class_id zalog_class_id,
      cz.c_inn zalog_inn
        from (
          select *
            from     $Node1t_team_k7m_aux_d_k7m_link_for_5212aIN
        where zalog_acc like '${SparkMainClass.zalogAcc}' ---Поручительства
  )   d
  right join    $Node2t_team_k7m_aux_d_k7m_link_for_5212aIN p
    on p.cred_id = d.cred_id
  join $Node3t_team_k7m_aux_d_k7m_link_for_5212aIN ca
    on ca.cred_id = p.cred_id
  join
  $Node4t_team_k7m_aux_d_k7m_link_for_5212aIN    cc
    on cc.id=p.client_id
  join      (
    select distinct org_inn_crm_num org_inn from $Node5t_team_k7m_aux_d_k7m_link_for_5212aIN
      where org_inn_crm_num is not null) b
  on b.org_inn =  cc.c_inn
  left join
    $Node4t_team_k7m_aux_d_k7m_link_for_5212aIN    cz
    on cz.id=d.zalog_client_id

  ) x""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_link_for_5212aOUT")

    logInserted()
    logEnd()
  }
}
