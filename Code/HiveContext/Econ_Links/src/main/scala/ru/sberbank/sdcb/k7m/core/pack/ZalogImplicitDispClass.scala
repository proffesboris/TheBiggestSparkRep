package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ZalogImplicitDispClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_zalog_implicit_dispIN = s"${DevSchema}.k7m_zalog_undisp"
  val Node2t_team_k7m_aux_d_k7m_zalog_implicit_dispIN = s"${DevSchema}.k7m_zalog_explisit_disp"
  val Node3t_team_k7m_aux_d_k7m_zalog_implicit_dispIN = s"${DevSchema}.k7m_cred_bal_total"
  val Nodet_team_k7m_aux_d_k7m_zalog_implicit_dispOUT = s"${DevSchema}.k7m_zalog_implicit_disp"
  val dashboardPath = s"${config.auxPath}k7m_zalog_implicit_disp"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_zalog_implicit_dispOUT //витрина
  override def processName: String = "ZALOG"

  def DoZalogImplDisp() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_zalog_implicit_dispOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  cred_id,
  zalog_id,
  undisp_zalog_sum,
  cred_sum,
  total_cred_sum,
  cast((undisp_zalog_sum*cred_sum)/total_cred_sum as decimal(17,2)) dispensed_sum
    from (
      select
        d.cred_id,
  d.zalog_id,
  --z.undisp_zalog_sum,
  cast(c.C_BALANCE_LCL as double)  cred_sum,
  cast(sum(c.C_BALANCE_LCL) over ( partition by d.zalog_id) as double) total_cred_sum,
  cast(z.undisp_zalog_sum as double) undisp_zalog_sum
    from $Node1t_team_k7m_aux_d_k7m_zalog_implicit_dispIN z
    join $Node2t_team_k7m_aux_d_k7m_zalog_implicit_dispIN d
    on z.zalog_id = d.zalog_id
  join $Node3t_team_k7m_aux_d_k7m_zalog_implicit_dispIN c
    on c.cred_id = d.cred_id
  where  d.zalog_disp_sum is  null
  and C_BALANCE_LCL>0
  ) x""").write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_zalog_implicit_dispOUT")

    logInserted()
    logEnd()
  }

}
