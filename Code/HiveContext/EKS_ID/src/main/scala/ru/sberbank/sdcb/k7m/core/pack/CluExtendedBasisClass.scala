package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CluExtendedBasisClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob{

  val Stg0Schema = config.stg
  val DevSchema = config.aux


  val Node1t_team_k7m_aux_d_clu_extended_basisIN = s"$DevSchema.lk"
  val Node2t_team_k7m_aux_d_clu_extended_basisIN = s"$DevSchema.clu_keys"
  val Node3t_team_k7m_aux_d_clu_extended_basisIN = s"$DevSchema.basis_client"
  val Nodet_team_k7m_aux_d_clu_extended_basisOUT = s"$DevSchema.clu_extended_basis"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clu_extended_basisOUT //витрина
  val dashboardPath = s"${config.auxPath}clu_extended_basis"
  override def processName: String = "clu_extended_basis"

  def DoCLUExtBasis() {

    Logger.getLogger(Nodet_team_k7m_aux_d_clu_extended_basisOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
    u7m_id,
    case when max(is_basis)=1 then 'Y'
    else 'N'
    end FLAG_BASIS_CLIENT
from  (
    select
        u7m_id_from as u7m_id,
        0 as is_basis
    from $Node1t_team_k7m_aux_d_clu_extended_basisIN
    where t_from ='CLU'
    union all
    select
        u7m_id_to as u7m_id,
        0 as is_basis
    from $Node1t_team_k7m_aux_d_clu_extended_basisIN
    where t_to ='CLU'
    union all
    select
        bs.u7m_id,
        1 as is_basis
    from $Node2t_team_k7m_aux_d_clu_extended_basisIN  bs
    join $Node3t_team_k7m_aux_d_clu_extended_basisIN bb
    on bb.org_inn_crm_num = bs.inn
    ) x
group by u7m_id"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_extended_basisOUT")

    logInserted()
    logEnd()
  }

}