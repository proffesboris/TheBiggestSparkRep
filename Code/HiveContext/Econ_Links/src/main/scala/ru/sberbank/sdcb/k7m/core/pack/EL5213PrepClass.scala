package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5213PrepClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL_5213_prepIN = s"${DevSchema}.k7m_EL_5213_issues"
  val Node2t_team_k7m_aux_d_k7m_EL_5213_prepIN = s"${MartSchema}.pdeksin"//s"${Stg0Schema}.z_main_kras_new"
  val Nodet_team_k7m_aux_d_k7m_EL_5213_prepOUT = s"${DevSchema}.k7m_EL_5213_prep"
  val dashboardPath = s"${config.auxPath}k7m_EL_5213_prep"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL_5213_prepOUT //витрина
  override def processName: String = "EL"

  def DoEL5213Prep() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL_5213_prepOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      bprov.inn_sec from_inn,
      issues.c_inn to_inn,
      issues.cred_id,
      issues.issue_amt,
      issues.issue_dt,
      issues.c_ft_credit,
      bprov.predicted_value  predicted_value,
      bprov.c_date_prov,
      bprov.id prov_id,
      cast(bprov.c_sum_nt as double) * cast(issues.issue_amt/sum(issues.issue_amt)  over (partition by bprov.id) as double) abs_amt
  from $Node1t_team_k7m_aux_d_k7m_EL_5213_prepIN issues
  join $Node2t_team_k7m_aux_d_k7m_EL_5213_prepIN bprov
    on bprov.inn_st = issues.c_inn
 where bprov.ktdt=1 --Клиенты базиса в Дебете
   and cast(c_date_prov as timestamp) between  issues.issue_dt and date_add(cast(issues.issue_dt as timestamp), 30)
   and bprov.predicted_value in (${SparkMainClass.includeYpred}) """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5213_prepOUT")

    logInserted()
    logEnd()
  }
}
