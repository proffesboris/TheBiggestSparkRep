package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class BasisEksClZalogClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val DevSchema = config.aux
  val Stg0Schema = config.stg

  //------------------ЕКС - обеспечение (z_zalog)-----------------------------

  val Node1t_team_k7m_aux_d_basis_client_zalogIN = s"${Stg0Schema}.eks_z_product"
  val Node2t_team_k7m_aux_d_basis_client_zalogIN = s"${Stg0Schema}.eks_z_zalog"
  val Node3t_team_k7m_aux_d_basis_client_zalogIN = s"${Stg0Schema}.eks_z_com_status_prd"
  val Node4t_team_k7m_aux_d_basis_client_zalogIN = s"${Stg0Schema}.eks_z_ac_fin"
  val Node5t_team_k7m_aux_d_basis_client_zalogIN = s"${DevSchema}.basis_client_eks"
  val Nodet_team_k7m_aux_d_basis_client_zalogOUT = s"${DevSchema}.basis_client_zalog"
  val dashboardPath = s"${config.auxPath}basis_client_zalog"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_zalogOUT //витрина
  override def processName: String = "Basis"

  def DoBasisEksClZalog()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_zalogOUT).setLevel(Level.WARN)
    logStart()

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
      select
         					 g.c_user_zalog as client_id,
         				   c.c_name as c_client_name,
                   c.C_INN as c_client_inn,
         				   p.C_NUM_DOG as num_dog,
                   f.C_MAIN_V_ID as num_acc,
                   f.C_SALDO as acc_saldo,
         				   p.C_DATE_BEGINING as prod_date_begining,
                   p.C_DATE_ENDING as prod_date_endining,
         			     s.C_code as prod_code,
                   s.C_NAME as prod_state,
                   p.C_DATE_CLOSE as dog_date_close
      FROM $Node1t_team_k7m_aux_d_basis_client_zalogIN p
         				join $Node2t_team_k7m_aux_d_basis_client_zalogIN g on upper(p.CLASS_ID) = 'ZALOG' and g.ID = p.ID
         				join $Node5t_team_k7m_aux_d_basis_client_zalogIN c on c.id = g.c_user_zalog
         				left join $Node3t_team_k7m_aux_d_basis_client_zalogIN s on p.c_com_status = s.ID and  s.C_code in ('TO_CLOSE', 'WORK', 'IN_SUPPORT', 'TO_BUH_CONTROL')
         				left join $Node4t_team_k7m_aux_d_basis_client_zalogIN f  on g.C_acc_zalog = f.ID
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_zalogOUT")

    logInserted()
    logEnd()
  }
}