package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ZalogUndispClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_zalog_undispIN = s"${DevSchema}.k7m_z_zalog_sum"
  val Node2t_team_k7m_aux_d_k7m_zalog_undispIN = s"${DevSchema}.k7m_zalog_explisit_disp"
  val Nodet_team_k7m_aux_d_k7m_zalog_undispOUT = s"${DevSchema}.k7m_zalog_undisp"
  val dashboardPath = s"${config.auxPath}k7m_zalog_undisp"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_zalog_undispOUT //витрина
  override def processName: String = "ZALOG"

  def DoZalogUndisp() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_zalog_undispOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  zalog_id,
  sum(zalog_sum) undisp_zalog_sum
    from (
      select id zalog_id,zalog_sum  from $Node1t_team_k7m_aux_d_k7m_zalog_undispIN
        union all
        select zalog_id,-zalog_disp_sum  zalog_sum from $Node2t_team_k7m_aux_d_k7m_zalog_undispIN
  ) x
  group by zalog_id
  having sum(zalog_sum)>0""").write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_zalog_undispOUT")

    logInserted()
    logEnd()
  }
}
