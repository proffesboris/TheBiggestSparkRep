package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class AccSvodClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_acc_svodIN = s"${DevSchema}.acc_client"
  val Node2t_team_k7m_aux_d_acc_svodIN = s"${DevSchema}.acc_product"
   val Nodet_team_k7m_aux_d_acc_svodOUT = s"${DevSchema}.acc_svod"
  val dashboardPath = s"${config.auxPath}acc_svod"
 

  override val dashboardName: String = Nodet_team_k7m_aux_d_acc_svodOUT //витрина
  override def processName: String = "ACC"

  def DoAccSvod() {

    Logger.getLogger(Nodet_team_k7m_aux_d_acc_svodOUT).setLevel(Level.WARN)
    logStart()

     val createHiveTableStage1 = spark.sql(
      s"""   select                f.ACC_ID,     --/*уникальный id в internal_eks_ibs.z_ac_fin*/
                                   f.ACC_NO,     --/*номер счета*/
                                   f.ACC_CURRENCY_ID, --/*уникальный id в internal_eks_ibs.z_ft_money*/
                                   f.ACC_CURRENCY, --/*Валюта счета*/
                                   f.ACC_NAME,    --/*наименование счета*/
                                   f.ACC_DATE_OP,   --/*дата открытия счета*/
                                   f.ACC_DATE_CLOSE, --/*дата закрытия счета*/
                                   p.ACC_D_NO, --/*номер договора РКО, действующего на Дату расчета, в рамках которого открыт счет*/
                                   p.ACC_D_DT, -- /*Дата договора РКО, действующего на Дату расчета,  в рамках которого открыт счет*/
                                   p.ACC_D_Close_DT, -- /*Дата закрытия договора РКО, действующего на Дату расчета,  в рамках которого открыт счет*/
                                   p.status_prod, --/*статус договора*/
                                   f.TB_ID, --/*уникальный id в internal_eks_ibs.z_branch*/
                                   f.TB_CODE, -- ТБ балансовой принадлежности счета
                                   f.DEPART_ID, --/*уникальный id в internal_eks_ibs.z_depart*/
                                   f.DEPART_CODE,  -- Подразделение принадлежности счета
                                   f.c_saldo,     --/*остаток в валюте счета*/
                                   f.c_saldo_nt,   --/*остаток в национальной валюте*/
                                   f.CL_EKS_ID,       --/*уникальный id в core_internal_eks.z_client*/
                                   f.CL_EKS_NAME,      --/*Наименование организации*/
                                   f.CL_EKS_INN,    --/*ИНН организации*/
                                   f.ACC_BIK, --/*БИК банка*/
                                   f.flag_7m, --/*Флаг расчетного счета для выставления предложения в личном кабинете клиента*/
                                   p.flag_over, --/*Флаг счета на основании которого считался лимит*/
                                   p.OVR_LIMIT_SUM, --/*рассчитанное значение лимита (ЕКС)*/
                                   p.RAITING, --/*рейтинг клиента, если установлен (ЕКС)*/
                                   p.ACC_CRED_TURN, /*совокупный объем кредитовых оборотов по счету*/
                                   p.ACC_GUID, /*ИД счета ЕКС для СББОЛ*/
                                   f.c_aim /*цель */
         from $Node1t_team_k7m_aux_d_acc_svodIN f
         join $Node2t_team_k7m_aux_d_acc_svodIN p on f.acc_id = p.acc_id
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_acc_svodOUT")

    logInserted()
    logEnd()
  }
}


