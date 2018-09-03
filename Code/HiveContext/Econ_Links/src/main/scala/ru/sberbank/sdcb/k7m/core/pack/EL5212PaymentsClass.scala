package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5212PaymentsClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_payments_for_5212IN = s"${DevSchema}.k7m_z_all_cred"
  val Node2t_team_k7m_aux_d_k7m_payments_for_5212IN = s"${DevSchema}.k7m_z_fact_oper_issues"
  val Nodet_team_k7m_aux_d_k7m_payments_for_5212OUT = s"${DevSchema}.k7m_payments_for_5212"
  val dashboardPath = s"${config.auxPath}k7m_payments_for_5212"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_payments_for_5212OUT //витрина
  override def processName: String = "EL"

  def DoEL5212Payments(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_payments_for_5212OUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      coalesce(c.c_high_level_cr,c.id) cred_id,
      max(c.c_client) client_id,
      coalesce(min(
      case when i.oper_type ='${SparkMainClass.excludeOperType}' then
      c_date
      else null
      end
      ),
      cast(add_months(date'${dateString}',-5*3)as timestamp)
      )  issue_dt,
      sum(
      case when i.oper_type !='${SparkMainClass.excludeOperType}' then
      c_summa
      else null
      end
      ) payment_amt
  from $Node1t_team_k7m_aux_d_k7m_payments_for_5212IN c
  join $Node2t_team_k7m_aux_d_k7m_payments_for_5212IN i
    on c.c_list_pay = i.collection_id
 where i.c_date <= cast(date'${dateString}'as timestamp)
   and i.c_date>=cast(add_months(date'${dateString}',-5*3)as timestamp)
 group by     coalesce(c.c_high_level_cr,c.id)
having
      sum(
      case when i.oper_type !='${SparkMainClass.excludeOperType}' then
      c_summa
      else null
      end
      )>0""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_payments_for_5212OUT")

    logInserted()
    logEnd()
  }
}
