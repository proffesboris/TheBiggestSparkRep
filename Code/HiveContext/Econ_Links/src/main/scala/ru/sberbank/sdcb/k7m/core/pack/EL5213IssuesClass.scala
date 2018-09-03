package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5213IssuesClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL_5213_issuesIN = s"${DevSchema}.k7m_cred_bal_total"
  val Node2t_team_k7m_aux_d_k7m_EL_5213_issuesIN = s"${DevSchema}.k7m_z_all_cred"
  val Node3t_team_k7m_aux_d_k7m_EL_5213_issuesIN = s"${DevSchema}.k7m_z_fact_oper_issues"
  val Node4t_team_k7m_aux_d_k7m_EL_5213_issuesIN = s"${Stg0Schema}.eks_z_client"
  val Node5t_team_k7m_aux_d_k7m_EL_5213_issuesIN = s"${DevSchema}.k7m_cur_courses"
  val Nodet_team_k7m_aux_d_k7m_EL_5213_issuesOUT = s"${DevSchema}.k7m_EL_5213_issues"
  val dashboardPath = s"${config.auxPath}k7m_EL_5213_issues"



  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL_5213_issuesOUT //витрина
  override def processName: String = "EL"

  def DoEL5213Issues(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL_5213_issuesOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  c.c_client,
  cl.c_inn,
  max(c.c_ft_credit) c_ft_credit,
  coalesce(c.c_high_level_cr,c.id) cred_id,
  i.issue_dt,
  sum(c_summa*coalesce(crs.course,1))  issue_amt,
  max(crs.course) course
    from $Node1t_team_k7m_aux_d_k7m_EL_5213_issuesIN b
    join $Node2t_team_k7m_aux_d_k7m_EL_5213_issuesIN c
    on c.id = b.cred_id
  join
  (
    select distinct collection_id,c_date issue_dt,     c_summa
    from $Node3t_team_k7m_aux_d_k7m_EL_5213_issuesIN
    where oper_type ='${SparkMainClass.excludeOperType}'
  and    c_date<=cast(date'${dateString}'as timestamp)
  and    c_date>=cast(add_months(date'${dateString}',-5*3) as timestamp)
  ) i
  on i.collection_id = c.c_list_pay
  join $Node4t_team_k7m_aux_d_k7m_EL_5213_issuesIN cl
    on cl.id = c.c_client
  join $Node5t_team_k7m_aux_d_k7m_EL_5213_issuesIN crs
    on c.c_ft_credit = crs.id
  where   i.issue_dt  between  crs.from_dt and crs.to_dt
  group by
    c.c_client,
  cl.c_inn,
  i.issue_dt,
  coalesce(c.c_high_level_cr,c.id)""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5213_issuesOUT")

    logInserted()
    logEnd()
  }
}
