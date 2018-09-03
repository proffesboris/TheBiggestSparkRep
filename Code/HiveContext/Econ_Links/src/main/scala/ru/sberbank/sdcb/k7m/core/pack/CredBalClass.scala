package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CredBalClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_cred_balIN = s"${Stg0Schema}.eks_Z_hoz_Op_Acc"
  val Node2t_team_k7m_aux_d_k7m_cred_balIN = s"${Stg0Schema}.eks_z_ac_fin"
  val Node3t_team_k7m_aux_d_k7m_cred_balIN = s"${DevSchema}.k7m_acc_rest"
  val Node4t_team_k7m_aux_d_k7m_cred_balIN = s"${Stg0Schema}.eks_z_product"
  val Node5t_team_k7m_aux_d_k7m_cred_balIN = s"${DevSchema}.k7m_z_all_cred"
  val Nodet_team_k7m_aux_d_k7m_cred_balOUT = s"${DevSchema}.k7m_cred_bal"
  val dashboardPath = s"${config.auxPath}k7m_cred_bal"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_cred_balOUT //витрина
  override def processName: String = "CRED_LIAB"

  def DoCredBal() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_cred_balOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""
  select
  distinct
  p.id cred_id,
  ac.id acc_id,
  r.C_BALANCE_LCL,
  r.C_BALANCE,
  max(oc.C_NAME_ACCOUNT) C_NAME_ACCOUNT
  from
  $Node1t_team_k7m_aux_d_k7m_cred_balIN oc
    join  $Node2t_team_k7m_aux_d_k7m_cred_balIN ac
    on ac.id = oc.C_ACCOUNT_DOG_1_2
  join $Node3t_team_k7m_aux_d_k7m_cred_balIN r
    on ac.c_arc_move = r.c_arc_move
  join $Node4t_team_k7m_aux_d_k7m_cred_balIN p
    on oc.collection_id = p.C_ARRAY_DOG_ACC
  where oc.C_NAME_ACCOUNT in ( ${SparkMainClass.cNameAccount} )
  and r.C_BALANCE_LCL<>0
 group by p.id,
         ac.id,
         r.C_BALANCE_LCL,
         r.C_BALANCE """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_cred_balOUT")

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_cred_balOUT+"_append").setLevel(Level.WARN)

    logInserted()

    val createHiveTableStage2 = spark.sql(
      s"""
  select  distinct
  crh.c_high_level_cr cred_id,
  ac.id acc_id,
  r.C_BALANCE_LCL,
  r.C_BALANCE,
  max(oc.C_NAME_ACCOUNT) C_NAME_ACCOUNT
  from
  $Node1t_team_k7m_aux_d_k7m_cred_balIN oc
    join  $Node2t_team_k7m_aux_d_k7m_cred_balIN ac
    on ac.id = oc.C_ACCOUNT_DOG_1_2
  join $Node3t_team_k7m_aux_d_k7m_cred_balIN r
    on ac.c_arc_move = r.c_arc_move
  join $Node4t_team_k7m_aux_d_k7m_cred_balIN p
    on oc.collection_id = p.C_ARRAY_DOG_ACC
  join $Node5t_team_k7m_aux_d_k7m_cred_balIN crh
    on p.id = crh.id
  where oc.C_NAME_ACCOUNT in ( ${SparkMainClass.cNameAccount} )
    and r.C_BALANCE_LCL<>0
  group by crh.c_high_level_cr,
          ac.id,
          r.C_BALANCE_LCL,
          r.C_BALANCE     """
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_cred_balOUT")

    logInserted()
    logEnd()
  }
}
