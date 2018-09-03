package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CredToAccClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_cred_to_accIN = s"${Stg0Schema}.eks_Z_hoz_Op_Acc"
  val Node2t_team_k7m_aux_d_k7m_cred_to_accIN = s"${Stg0Schema}.eks_z_ac_fin"
  val Node3t_team_k7m_aux_d_k7m_cred_to_accIN = s"${Stg0Schema}.eks_z_product"
  val Node4t_team_k7m_aux_d_k7m_cred_to_accIN = s"${DevSchema}.k7m_z_all_cred"
  val Nodet_team_k7m_aux_d_k7m_cred_to_accOUT = s"${DevSchema}.k7m_cred_to_acc"
  val dashboardPath = s"${config.auxPath}k7m_cred_to_acc"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_cred_to_accOUT //витрина
  override def processName: String = "EL"

  def DoCredToAcc() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_cred_to_accOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s""" select
  coalesce(c.c_high_level_cr,c.id) cred_id,
  ac.id acc_id,
  max(oc.C_NAME_ACCOUNT) C_NAME_ACCOUNT
    from $Node1t_team_k7m_aux_d_k7m_cred_to_accIN oc
    join $Node2t_team_k7m_aux_d_k7m_cred_to_accIN ac
    on ac.id = oc.C_ACCOUNT_DOG_1_2
  join $Node3t_team_k7m_aux_d_k7m_cred_to_accIN p
    on oc.collection_id = p.C_ARRAY_DOG_ACC
  join   $Node4t_team_k7m_aux_d_k7m_cred_to_accIN   c
    on c.id = p.id
  where  oc.C_NAME_ACCOUNT in ( ${SparkMainClass.cNameAccount} )
  group by
    coalesce(c.c_high_level_cr,c.id),
  ac.id """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_cred_to_accOUT")

    logInserted()
    logEnd()
  }
}
