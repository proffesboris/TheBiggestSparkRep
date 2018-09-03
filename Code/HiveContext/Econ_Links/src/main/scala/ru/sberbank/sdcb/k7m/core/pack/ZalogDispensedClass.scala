package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ZalogDispensedClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_zalog_dispensedIN = s"${DevSchema}.k7m_zalog_explisit_disp"
  val Node2t_team_k7m_aux_d_k7m_zalog_dispensedIN = s"${DevSchema}.k7m_zalog_implicit_disp"
  val Nodet_team_k7m_aux_d_k7m_zalog_dispensedOUT = s"${DevSchema}.k7m_zalog_dispensed"
  val dashboardPath = s"${config.auxPath}k7m_zalog_dispensed"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_zalog_dispensedOUT //витрина
  override def processName: String = "ZALOG"

  def DoZalogDisp() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_zalog_dispensedOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  ze.class_id,
  ze.cred_id,
  ze.zalog_id,
  ze.cred_client_id,
  ze.zalog_client_id,
  coalesce(ze.zalog_disp_sum,zi.dispensed_sum) final_dispensed_sum,
  ze.zalog_disp_sum e_zalog_disp_sum,
  zi.dispensed_sum i_zalog_disp_sum,
  zi.cred_sum,
  zi.undisp_zalog_sum,
  ze.zalog_acc
  from $Node1t_team_k7m_aux_d_k7m_zalog_dispensedIN ze
    left join $Node2t_team_k7m_aux_d_k7m_zalog_dispensedIN zi
    on ze.cred_id = zi.cred_id
  and ze.zalog_id =zi.zalog_id
  where  coalesce(ze.zalog_disp_sum,zi.dispensed_sum) > 0""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_zalog_dispensedOUT")

    logInserted()
    logEnd()
  }
}
