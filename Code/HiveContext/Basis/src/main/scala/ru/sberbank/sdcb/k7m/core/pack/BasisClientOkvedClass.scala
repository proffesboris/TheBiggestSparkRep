package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class BasisClientOkvedClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val DevSchema = config.aux
  val Stg0Schema = config.stg

  //-----------------------Integrum_pravo-----------------
  val Node1t_team_k7m_aux_d_basis_client_okvedIN = s"${Stg0Schema}.int_org_okved_egrul_egrip"
  val Node2t_team_k7m_aux_d_basis_client_okvedIN = s"${DevSchema}.basis_client_crm"
  val Node3t_team_k7m_aux_d_basis_client_okvedIN = s"${DevSchema}.basis_integr_egrul"
  val Nodet_team_k7m_aux_d_basis_client_okvedOUT = s"${DevSchema}.basis_client_okved"
  val dashboardPath = s"${config.auxPath}basis_client_okved"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_okvedOUT //витрина
  override def processName: String = "Basis"

  def DoBasisClientOkved()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_okvedOUT).setLevel(Level.WARN)
    logStart()


    val smartSrcHiveTable_t7=spark.sql(s"""
select crm.org_id as org_crm_id,
 okved.okvd_okved_cd as okveds_egrul_name,
         		okved.okvd_main_flg as okveds_egrul_flag
         from $Node2t_team_k7m_aux_d_basis_client_okvedIN crm
         inner join $Node3t_team_k7m_aux_d_basis_client_okvedIN egrul on egrul.ul_inn=crm.org_inn
         inner join $Node1t_team_k7m_aux_d_basis_client_okvedIN okved on egrul.org_id=okved.egrul_org_id
 and okved.is_active_in_period='Y' and year(okved.effectiveto)=2999
       """
    )
    smartSrcHiveTable_t7.write.format("parquet").mode("overwrite").option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_okvedOUT")

    logInserted()
    logEnd()
  }

}
