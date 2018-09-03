package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class AccClientClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_acc_clientIN = s"${Stg0Schema}.eks_z_ac_fin"
  val Node2t_team_k7m_aux_d_acc_clientIN = s"${Stg0Schema}.eks_z_client"
  val Node3t_team_k7m_aux_d_acc_clientIN = s"${Stg0Schema}.eks_z_intend_for"
  val Node4t_team_k7m_aux_d_acc_clientIN = s"${Stg0Schema}.eks_z_ft_money"
  val Node5t_team_k7m_aux_d_acc_clientIN = s"${Stg0Schema}.eks_z_branch"
  val Node6t_team_k7m_aux_d_acc_clientIN = s"${Stg0Schema}.eks_z_depart"
  val Node7t_team_k7m_aux_d_acc_clientIN = s"${Stg0Schema}.eks_z_cl_part"
  val Node8t_team_k7m_aux_d_acc_clientIN = s"${Stg0Schema}.eks_z_cl_bank_n"
   val Nodet_team_k7m_aux_d_acc_clientOUT = s"${DevSchema}.acc_client"
  val dashboardPath = s"${config.auxPath}acc_client"
 

  override val dashboardName: String = Nodet_team_k7m_aux_d_acc_clientOUT //витрина
  override def processName: String = "ACC"

  def DoAccClient() {

    Logger.getLogger(Nodet_team_k7m_aux_d_acc_clientOUT).setLevel(Level.WARN)
    logStart()

     val createHiveTableStage1 = spark.sql(
      s""" select            f.id as ACC_ID,     --уникальный id в internal_eks_ibs.z_ac_fin
                                      f.c_main_v_id as ACC_NO,     --номер счета
                                      f.c_fintool as ACC_CURRENCY_ID, --уникальный id в internal_eks_ibs.z_ft_money
                                      m.c_cur_short as ACC_CURRENCY, --Валюта счета                   -- m.c_code_iso Изменено 19.07.2018 по требования Е.Шиловой SDCB-409
                                      f.c_name as ACC_NAME,    --наименование счета
                                      cast(f.c_date_op as timestamp) as ACC_DATE_OP,   --дата открытия счета
                                      cast(f.c_date_close as timestamp) as ACC_DATE_CLOSE, --дата закрытия счета
                                      f.c_filial as TB_ID, --уникальный id в internal_eks_ibs.z_branch
                                      b.c_local_code as TB_CODE, -- ТБ балансовой принадлежности счета
                                      f.c_depart as DEPART_ID, --уникальный id в internal_eks_ibs.z_depart
                                      d.c_code as DEPART_CODE,  -- Подразделение принадлежности счета
               --                     f.c_com_status as ACC_STATUS_ID,   --уникальный id в internal_eks_ibs.z_com_status_prd
               --                     s.c_name,   --статус счета
                                      f.c_saldo,     --остаток в валюте счета
                                      f.c_saldo_nt,   --остаток в национальной валюте
                                      cast(cast(c.ID as bigint) as string) as CL_EKS_ID,       --уникальный id в core_internal_eks.z_client
                                      c.c_name as CL_EKS_NAME,      --Наименование организации
                                      c.c_inn as CL_EKS_INN,    --ИНН организации
                                      cn.c_bic as ACC_BIK, --БИК банка
                                      case when (substr(f.c_main_v_id,1,3) between '405%' and '407%') then '1'
                                       else  '0' end as flag_7m, --Флаг расчетного счета для выставления предложения в личном кабинете клиента
                                      lower(i.c_aim) as c_aim
         from $Node1t_team_k7m_aux_d_acc_clientIN f
           join $Node2t_team_k7m_aux_d_acc_clientIN c on f.c_client_v=c.id
           join $Node3t_team_k7m_aux_d_acc_clientIN i on f.c_target = i.id
           join $Node4t_team_k7m_aux_d_acc_clientIN m on m.id = f.c_fintool
           join $Node5t_team_k7m_aux_d_acc_clientIN b on f.c_filial = b.id
           join $Node6t_team_k7m_aux_d_acc_clientIN d on f.c_depart = d.id
           join $Node7t_team_k7m_aux_d_acc_clientIN cp on cp.id = d.c_part
           join $Node8t_team_k7m_aux_d_acc_clientIN cn on cn.id = cp.c_bank
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_acc_clientOUT")

    logInserted()
    logEnd()
  }
}


