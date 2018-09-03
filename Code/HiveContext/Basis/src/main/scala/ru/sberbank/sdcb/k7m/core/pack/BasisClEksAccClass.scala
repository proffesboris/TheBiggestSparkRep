package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class BasisClEksAccClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val DevSchema = config.aux
  val Stg0Schema = config.stg

  //------------------EKS-----------------------------
  val Node1t_team_k7m_aux_d_basis_client_accIN = s"${Stg0Schema}.eks_z_ac_fin"
  val Node2t_team_k7m_aux_d_basis_client_accIN = s"${Stg0Schema}.eks_z_com_status_prd"
  val Node3t_team_k7m_aux_d_basis_client_accIN = s"${DevSchema}.basis_client_eks"
  val Nodet_team_k7m_aux_d_basis_client_accOUT = s"${DevSchema}.basis_client_acc"
  val dashboardPath = s"${config.auxPath}basis_client_acc"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_accOUT //витрина
  override def processName: String = "Basis"

  def DoBasisClEksAcc()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_accOUT).setLevel(Level.WARN)
    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
select c.id as cl_id,						--уникальный id в core_internal_eks.z_client
         		   c.c_name as cl_name,					--Наименование организации
         		   c.c_inn as inn,						--ИНН организации
         		   f.id as acc_id,						--уникальный id в internal_eks_ibs.z_ac_fin
         		   f.c_main_v_id as acc_num,			--номер счета
         		   f.c_name as acc_name, 				--наименование счета
         		   f.c_date_op as acc_date_op,  	--дата открытия счета
         		   f.c_date_close as acc_date_close,  	--дата закрытия счета
         		   f.c_com_status as acc_com_status,  --уникальный id в internal_eks_ibs.z_com_status_prd
         		   s.c_name,  							--статус счета
         		   f.c_saldo,  							--остаток в валюте счета
         		   f.c_saldo_nt  						--остаток в национальной валюте
         from $Node3t_team_k7m_aux_d_basis_client_accIN c
         join $Node1t_team_k7m_aux_d_basis_client_accIN f on f.c_client_v=c.id
         left join $Node2t_team_k7m_aux_d_basis_client_accIN s on s.id=f.c_com_status
         where (f.c_date_close is null or f.c_date_close>current_date()) and f.id is not null  		--Проверяем, что дата закрытия счета пустая или больше текущей даты
         and f.c_main_v_id rlike "^40[567][0-9]*"
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_accOUT")

    logInserted()
    logEnd()
  }
}
