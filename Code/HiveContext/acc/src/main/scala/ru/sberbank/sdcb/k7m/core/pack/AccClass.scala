package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class AccClass(val spark: SparkSession, val config: Config, val date: String) extends EtlLogger with EtlJob {

  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_pa_d_accIN = s"${DevSchema}.acc_svod"
  val Node2t_team_k7m_pa_d_accIN = s"${DevSchema}.clu_base"
   val Nodet_team_k7m_pa_d_accOUT = s"${MartSchema}.acc"
  val dashboardPath = s"${config.paPath}acc"
 

  override val dashboardName: String = Nodet_team_k7m_pa_d_accOUT //витрина
  override def processName: String = "ACC"

  def DoAcc() {

    Logger.getLogger(Nodet_team_k7m_pa_d_accOUT).setLevel(Level.WARN)
    logStart()

     val createHiveTableStage1 = spark.sql(
      s"""select                   c.u7m_id,            --уникальный идентификатор клиента процесса К7М
                                   ACC_ID,              --уникальный id в internal_eks_ibs.z_ac_fin
                                   ACC_NO,              --номер счета
                                   ACC_CURRENCY_ID,     --уникальный id в internal_eks_ibs.z_ft_money
                                   ACC_CURRENCY,        --Валюта счета
                                   ACC_NAME,            --наименование счета
                                   ACC_DATE_OP,         --дата открытия счета
                                   ACC_DATE_CLOSE,      --дата закрытия счета
                                   ACC_D_NO,            --номер договора РКО, действующего на Дату расчета, в рамках которого открыт счет
                                   ACC_D_DT,            --Дата договора РКО, действующего на Дату расчета,  в рамках которого открыт счет
                                   status_prod,         --статус договора
                                   TB_ID,               --уникальный id в internal_eks_ibs.z_branch
                                   TB_CODE,             --ТБ балансовой принадлежности счета
                                   DEPART_ID,           --уникальный id в internal_eks_ibs.z_depart
                                   DEPART_CODE,         --Подразделение принадлежности счета
                                   c_saldo,             --остаток в валюте счета
                                   c_saldo_nt,          --остаток в национальной валюте
                                   CL_EKS_ID,           --уникальный id в core_internal_eks.z_client
                                   CL_EKS_NAME,         --Наименование организации
                                   CL_EKS_INN,          --ИНН организации
                                   ACC_BIK,             --БИК банка
                                   flag_7m,             --Флаг расчетного счета для выставления предложения в личном кабинете клиента
                                   flag_over,           --Флаг счета на основании которого считался лимит
                                   OVR_LIMIT_SUM,       --рассчитанное значение лимита (ЕКС)
                                   RAITING,             --рейтинг клиента, если установлен (ЕКС)
                                   ACC_CRED_TURN,       --совокупный объем кредитовых оборотов по счету
                                   ACC_GUID,            --ИД счета ЕКС для СББОЛ
                                   c_aim                --цель
         from   $Node1t_team_k7m_pa_d_accIN  a
           join $Node2t_team_k7m_pa_d_accIN c on a.CL_EKS_ID = c.eks_id
         where  ((ACC_D_Close_DT is null) or (ACC_D_Close_DT > cast('$date' as timestamp))) -- current_date() заменить на параметр Бизнес-дата расчета
            and (status_prod is not null and status_prod not in ('TO_OPEN','OPEN_CLOSE','CLOSE'))
            and (acc_no like '40502%' or acc_no like '40602%' or  acc_no like '40702%' )
            and  ACC_GUID is not null
            and  ACC_D_NO is not null
            and  acc_D_DT is not null
            and  ACC_CURRENCY is not null
            and  ACC_BIK is not null
            and c_aim not like '%транзит%'      --Должны исключаться счета, если в названии есть слово «транзитный». А так же  параметр «целевое назначение» = «транзитный».
            and length(regexp_replace(f.c_main_v_id,' ','')) = 20
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_pa_d_accOUT")

    logInserted()
    logEnd()
  }
}


