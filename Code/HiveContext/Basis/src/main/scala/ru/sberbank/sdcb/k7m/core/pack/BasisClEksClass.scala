package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisClEksClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux

  //------------------EKS-----------------------------

  val Node1t_team_k7m_aux_d_basis_client_eksIN = s"${Stg0Schema}.eks_z_client"
  val Node2t_team_k7m_aux_d_basis_client_eksIN = s"${Stg0Schema}.eks_z_cl_corp"
  val Node3t_team_k7m_aux_d_basis_client_eksIN = s"${Stg0Schema}.eks_z_form_property"
  val Node4t_team_k7m_aux_d_basis_client_eksIN = s"${DevSchema}.basis_client_crm"
  val Nodet_team_k7m_aux_d_basis_client_eksOUT = s"${DevSchema}.basis_client_eks"
  val dashboardPath = s"${config.auxPath}basis_client_eks"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_eksOUT //витрина
  override def processName: String = "Basis"

  def DoBasisClEks()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_eksOUT).setLevel(Level.WARN)
    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
       select  c.id,    --уникальный id в internal_eks_ibs.z_client
               c.c_name,  --Наименование организации
               c.c_inn,   --ИНН организации
               c.c_kpp,    --КПП организации
               cc.c_kod_okpo,
               cc.c_register_date_reestr,    -- @@@NEW_5	080518 - добавляю дату первичной регистрации
               fp.opf_id,
               fp.opf_code,
               fp.opf_code_new,
               fp.opf_short_name,
               fp.opf_name,
               fp.high_opf_code,
               fp.high_opf_code_new,
               fp.high_opf_short_name,
               fp.high_opf_name
       from  $Node1t_team_k7m_aux_d_basis_client_eksIN c
       join $Node2t_team_k7m_aux_d_basis_client_eksIN cc on cc.id=c.id
       left join (select fp1.id as opf_id,
                       fp1.c_code as opf_code,
                       fp1.c_code_new as opf_code_new,
                       fp1.c_short_name as opf_short_name,
                       fp1.c_name  as opf_name,
                       fp2.c_code as high_opf_code,
                       fp2.c_code_new as high_opf_code_new,
                       fp2.c_short_name as high_opf_short_name,
                       fp2.c_name  as high_opf_name
                    from $Node3t_team_k7m_aux_d_basis_client_eksIN fp1
                    left join $Node3t_team_k7m_aux_d_basis_client_eksIN fp2 on fp1.c_upper=fp2.id) fp on fp.opf_id=cc.c_forma
             where c.c_inn in (select distinct s.org_inn from $Node4t_team_k7m_aux_d_basis_client_eksIN s)
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_eksOUT")

    logInserted()
    logEnd()
  }
}
