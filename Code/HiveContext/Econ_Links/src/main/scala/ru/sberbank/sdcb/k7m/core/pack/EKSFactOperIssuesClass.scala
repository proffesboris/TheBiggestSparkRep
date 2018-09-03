package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EKSFactOperIssuesClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_z_fact_oper_issuesIN = s"${Stg0Schema}.eks_z_fact_oper"
  val Nodet_team_k7m_aux_d_k7m_z_fact_oper_issuesOUT = s"${DevSchema}.k7m_z_fact_oper_issues"
  val dashboardPath = s"${config.auxPath}k7m_z_fact_oper_issues"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_z_fact_oper_issuesOUT //витрина
  override def processName: String = "EL"

  def DoEKSFactOperIssues() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_z_fact_oper_issuesOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select id,
      collection_id,
      c_date,
      case
        when c_oper in (${SparkMainClass.listCOperIssue}) then 'ISSUE'
        else 'PAYMENT'
        end oper_type,
      c_summa
  from $Node1t_team_k7m_aux_d_k7m_z_fact_oper_issuesIN
  where  c_oper in (
    ${SparkMainClass.listCOperIssue},
    ${SparkMainClass.listCOperPayment}
  )
  --and        c_date between unix_timestamp(cast(add_months(date'2017-12-06',-12) as timestamp))*1000 and unix_timestamp(cast(date'2017-12-06'as timestamp))*1000
  """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_z_fact_oper_issuesOUT")

    logInserted()
    logEnd()
  }
}
