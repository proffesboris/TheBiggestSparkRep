package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ZalogToCredClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_zalog_to_credIN = s"${Stg0Schema}.eks_z_zalog"
  val Node2t_team_k7m_aux_d_k7m_zalog_to_credIN = s"${Stg0Schema}.eks_z_PART_TO_LOAN"
  val Node3t_team_k7m_aux_d_k7m_zalog_to_credIN = s"${DevSchema}.k7m_z_all_cred"
  val Nodet_team_k7m_aux_d_k7m_zalog_to_credOUT = s"${DevSchema}.k7m_zalog_to_cred"
  val dashboardPath = s"${config.auxPath}k7m_zalog_to_cred"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_zalog_to_credOUT //витрина
  override def processName: String = "ZALOG"

  def DoZalogToCred()
                    {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_zalog_to_credOUT).setLevel(Level.WARN)

  logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  DISTINCT
  p.class_id,
  p.id cred_id,
  z.id zalog_id,
  p.c_client cred_client_id,
  z.c_user_zalog zalog_client_id,
  cast(cast( pz.C_PART as double) as decimal(17,2)) C_PART,
  pz.C_PRC_ATTR
  from
  $Node1t_team_k7m_aux_d_k7m_zalog_to_credIN z
    JOIN $Node2t_team_k7m_aux_d_k7m_zalog_to_credIN pz
    ON z.C_PART_TO_LOAN = pz.COLLECTION_ID
  JOIN $Node3t_team_k7m_aux_d_k7m_zalog_to_credIN p
    on pz.c_product = p.id""").write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_zalog_to_credOUT")

                      logInserted()
                      logEnd()
  }
}
