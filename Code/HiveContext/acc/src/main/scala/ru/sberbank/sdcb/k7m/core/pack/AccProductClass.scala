package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class AccProductClass(val spark: SparkSession, val config: Config, val date: String) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_acc_productIN = s"${DevSchema}.acc_client"
  val Node2t_team_k7m_aux_d_acc_productIN = s"${Stg0Schema}.eks_z_rko"
  val Node3t_team_k7m_aux_d_acc_productIN = s"${Stg0Schema}.eks_z_product"
  val Node4t_team_k7m_aux_d_acc_productIN = s"${Stg0Schema}.eks_z_com_status_prd"
  val Node5t_team_k7m_aux_d_acc_productIN = s"${Stg0Schema}.eks_z_objects_guid"
  val Node6t_team_k7m_aux_d_acc_productIN = s"${Stg0Schema}.eks_z_over_res"
  val Node7t_team_k7m_aux_d_acc_productIN = s"${Stg0Schema}.eks_z_calc_over_lim"
   val Nodet_team_k7m_aux_d_acc_productOUT = s"${DevSchema}.acc_product"
  val dashboardPath = s"${config.auxPath}acc_product"
 

  override val dashboardName: String = Nodet_team_k7m_aux_d_acc_productOUT //витрина
  override def processName: String = "ACC"

  def DoAccProduct() {

    Logger.getLogger(Nodet_team_k7m_aux_d_acc_productOUT).setLevel(Level.WARN)
    logStart()

     val createHiveTableStage1 = spark.sql(
      s"""  select                ACC_ID,     --уникальный id в internal_eks_ibs.z_ac_fin
                                   case
                                     when (p.c_date_close is null or p.c_date_close > cast('$date' as timestamp)) --было current_date()
                                     then p.c_num_dog
                                  else null
                                  end as ACC_D_NO, --номер договора РКО, действующего на Дату расчета, в рамках которого открыт счет
                                  cast(case
                                     when (p.c_date_close is null or p.c_date_close > cast('$date' as timestamp)) --было current_date()
                                     then coalesce(p.c_date_begining,  p.c_date_begin)
                                     else null
                                  end as timestamp) as ACC_D_DT, -- Дата договора РКО, действующего на Дату расчета,  в рамках которого открыт счет
                                  cast(p.c_date_close as timestamp) as ACC_D_CLOSE_DT, -- Дата закрытия договора РКО, действующего на Дату расчета,  в рамках которого открыт счет
                                  s.c_code as STATUS_PROD,  --статус договора
                                  if(ovrl.c_ac_fin is not null,1,0) as flag_over, --Флаг счета на основании которого считался лимит
                                  ovrl.c_over_limit_res as OVR_LIMIT_SUM, --рассчитанное значение лимита (ЕКС)
                                  ovrl.c_raiting as RAITING, --рейтинг клиента, если установлен (ЕКС)
                                  ovrl.c_ac_fin_res as ACC_CRED_TURN, --совокупный объем кредитовых оборотов по счету
                                  g.c_guid as ACC_GUID --ИД счета ЕКС для СББОЛ
         from $Node1t_team_k7m_aux_d_acc_productIN f
           left join (select * from
                             (select c_account,r.id, p.c_date_begining, p.c_date_begin, p.c_date_ending, p.c_date_close, p.c_num_dog, p.c_com_status
                                     , row_number() over (partition by c_account order by coalesce(p.c_date_begining,p.c_date_begin) desc, r.id desc) rn
                               from $Node2t_team_k7m_aux_d_acc_productIN  r
                                   left join $Node3t_team_k7m_aux_d_acc_productIN p on r.id = p.id
                             ) t
                      where rn=1
           ) p on p.c_account = f.ACC_ID --нужно выбрать один договор из двух открытых!!!
           left join $Node4t_team_k7m_aux_d_acc_productIN s on s.id=p.c_com_status
           left join $Node5t_team_k7m_aux_d_acc_productIN g on g.c_object_id = f.ACC_ID  and c_class_id = 'AC_FIN'
           left join (select * from
           (select cast(concat(substr(c_date_calc,7,4),'-',substr(c_date_calc,4,2),'-',substr(c_date_calc,1,2), substr(c_date_calc,11)) as timestamp) as c_date_calc,
                   ovr.c_ac_fin, --cast(ovr.c_ac_fin as bigint) as c_ac_fin,
                   ovr.collection_id, --cast(ovr.collection_id as bigint) as collection_id,
                   ovrl.c_over_res,
                   ovrl.c_over_limit_res,
                   ovrl.c_raiting,
                   ovr.c_ac_fin_res,
         --          ovr.*, ovrl.*,
                   row_number() over (partition by ovr.c_ac_fin --cast(ovr.c_ac_fin as bigint)
                   order by cast(concat(substr(c_date_calc,7,4),'-',substr(c_date_calc,4,2),'-',substr(c_date_calc,1,2), substr(c_date_calc,11)) as timestamp) desc) rn
                     --, c_over_limit_res, c_over_res,c_over_limit_res, ovrl.collection_id,
                   from  $Node6t_team_k7m_aux_d_acc_productIN ovr
                 left join $Node7t_team_k7m_aux_d_acc_productIN ovrl on ovrl.c_over_res = ovr.collection_id --cast(ovrl.c_over_res as bigint) = cast(ovr.collection_id as bigint)
                 ) t
             where rn =1) ovrl on ovrl.c_ac_fin = f.ACC_ID
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_acc_productOUT")

    logInserted()
    logEnd()
  }
}


