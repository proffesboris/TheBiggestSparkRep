package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class AllCredClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_z_all_credIN = s"${Stg0Schema}.eks_z_pr_cred"
  val Node2t_team_k7m_aux_d_k7m_z_all_credIN = s"${Stg0Schema}.eks_z_guaranties"
  val Nodet_team_k7m_aux_d_k7m_z_all_credOUT = s"${DevSchema}.k7m_z_all_cred"
  val dashboardPath = s"${config.auxPath}k7m_z_all_cred"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_z_all_credOUT //витрина
  override def processName: String = "CRED_LIAB"

  def DoEKSAllCred() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_z_all_credOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select id,
      c_client,
      class_id,
      c_zalog,
      c_list_pay,
      c_num_dog,
      c_summa_dog,
      c_high_level_cr,
      c_ft_credit
  from $Node1t_team_k7m_aux_d_k7m_z_all_credIN"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_z_all_credOUT")

    logInserted()

    val createHiveTableStage2= spark.sql(
      s"""select id,
     c_PRINCIPAL c_client,
     'GUARANTIES' class_id,
      c_Security c_zalog,
      c_list_fact c_list_pay,
      c_guarantie_num c_num_dog,
      c_summa as c_summa_dog,
      cast(NULL as string) as c_high_level_cr,
      c_valuta as c_ft_credit
  from $Node2t_team_k7m_aux_d_k7m_z_all_credIN"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_z_all_credOUT")

    logInserted()
    logEnd()
  }
}
