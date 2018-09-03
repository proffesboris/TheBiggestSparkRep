package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ZalogExplicitDispClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_zalog_explisit_dispIN = s"${DevSchema}.k7m_zalog_to_cred"
  val Node2t_team_k7m_aux_d_k7m_zalog_explisit_dispIN = s"${DevSchema}.k7m_z_zalog_sum"
  val Nodet_team_k7m_aux_d_k7m_zalog_explisit_dispOUT = s"${DevSchema}.k7m_zalog_explisit_disp"
  val dashboardPath = s"${config.auxPath}k7m_zalog_explisit_disp"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_zalog_explisit_dispOUT //витрина
  override def processName: String = "ZALOG"

  def DoZalogExpDisp() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_zalog_explisit_dispOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
        zc.class_id,
        zc.cred_id,
        zc.zalog_id,
        zc.cred_client_id,
        zc.zalog_client_id,
        zs.zalog_acc,
        case when zc.C_PRC_ATTR = '1'
        and zc.c_part<=100 then
          (zc.c_part/100)*zs.zalog_sum
        when zc.C_PRC_ATTR = '0'    then
          zc.c_part
        else null
        end zalog_disp_sum
    from $Node1t_team_k7m_aux_d_k7m_zalog_explisit_dispIN zc
    join $Node2t_team_k7m_aux_d_k7m_zalog_explisit_dispIN zs
      on zc.zalog_id = zs.id""") .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_zalog_explisit_dispOUT")

    logInserted()
    logEnd()
  }
}
