package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class BasisEksClGenAgrFrameClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val DevSchema = config.aux
  val Stg0Schema = config.stg
  //------------------EKS Ген.соглашения об открытии рамочной кред.линии-----------------------------

  val Node1t_team_k7m_aux_d_basis_client_gen_agreem_frameIN = s"${Stg0Schema}.eks_z_product"
  val Node2t_team_k7m_aux_d_basis_client_gen_agreem_frameIN = s"${Stg0Schema}.eks_z_GEN_AGREEM_FRAME"
  val Node3t_team_k7m_aux_d_basis_client_gen_agreem_frameIN = s"${Stg0Schema}.eks_z_com_status_prd"
  val Node4t_team_k7m_aux_d_basis_client_gen_agreem_frameIN = s"${Stg0Schema}.eks_z_ft_money"
  val Node5t_team_k7m_aux_d_basis_client_gen_agreem_frameIN = s"${Stg0Schema}.eks_z_kind_credits"
  val Node6t_team_k7m_aux_d_basis_client_gen_agreem_frameIN = s"${Stg0Schema}.eks_z_types_cred"
  val Node7t_team_k7m_aux_d_basis_client_gen_agreem_frameIN = s"${Stg0Schema}.eks_z_ac_fin"
  val Node8t_team_k7m_aux_d_basis_client_gen_agreem_frameIN = s"${DevSchema}.basis_client_eks"
  val Nodet_team_k7m_aux_d_basis_client_gen_agreem_frameOUT = s"${DevSchema}.basis_client_gen_agreem_frame"
  val dashboardPath = s"${config.auxPath}basis_client_gen_agreem_frame"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_gen_agreem_frameOUT //витрина
  override def processName: String = "Basis"

  def DoBasisEksClGenAgrFrame()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_gen_agreem_frameOUT).setLevel(Level.WARN)
    logStart()

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
                   select
         				 g.c_client as client_id,         --Ссылка на id клиента
         				 c.c_name as c_client_name,     --Наименование клиента - ЮЛ, с которым заключен договор о банковской ганартии
                 c.C_INN as c_client_inn,       --ИНН клиента
         				 p.C_NUM_DOG as num_dog,		   --Номер договора
                 f.C_MAIN_V_ID as num_acc,         --Номер внебал счета
                 f.C_SALDO as acc_saldo,          --Остаток на внебал счете
         				 p.C_DATE_BEGINING as prod_date_begining,                 --Дата начала действия договора
                 p.C_DATE_ENDING as prod_date_endining,                                    --Дата окончания действия договора
         			   s.C_code as prod_code,               --Код статуса договора
                 s.C_NAME as prod_state,                           --Статус договора
                 g.C_SUMMA as dog_sum,                         --Сумма договора
                 ft1.C_CUR_SHORT as val_lim,    --Валюта договора
                 p.C_DATE_CLOSE as dog_date_close,           --Дата закрытия договора
                 kc.c_code as kind_code,             --Код вида договора
                 kc.c_name as kind_name,          --Вид договора
                 tc.c_short_name as type_name,     --Тип договора
          				s.C_code  									-- @@@NEW_3 Статус договора
                 FROM $Node1t_team_k7m_aux_d_basis_client_gen_agreem_frameIN p
                 join $Node2t_team_k7m_aux_d_basis_client_gen_agreem_frameIN g on upper(p.CLASS_ID) = 'GEN_AGREEM_FRAME' and g.ID = p.ID
                 join $Node8t_team_k7m_aux_d_basis_client_gen_agreem_frameIN c on c.id = g.c_client
                 left join $Node3t_team_k7m_aux_d_basis_client_gen_agreem_frameIN s on p.c_com_status = s.ID and  s.C_code in ('TO_CLOSE', 'WORK', 'IN_SUPPORT', 'TO_BUH_CONTROL')
                 left join $Node4t_team_k7m_aux_d_basis_client_gen_agreem_frameIN ft1  on g.C_VALUTA = ft1.ID
                 left join $Node5t_team_k7m_aux_d_basis_client_gen_agreem_frameIN kc  on g.C_KIND_CREDIT = kc.ID
                 left join $Node6t_team_k7m_aux_d_basis_client_gen_agreem_frameIN tc on kc.c_reg_rules=tc.id
                 left join $Node7t_team_k7m_aux_d_basis_client_gen_agreem_frameIN f  on g.C_VNB_UNUSED_LINE = f.ID
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_gen_agreem_frameOUT")

    logInserted()
    logEnd()

  }
}