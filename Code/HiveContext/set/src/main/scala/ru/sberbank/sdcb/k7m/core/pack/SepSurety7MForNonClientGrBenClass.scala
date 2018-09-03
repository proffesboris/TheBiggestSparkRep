package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SepSurety7MForNonClientGrBenClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benIN = s"${MartSchema}.clu"
    val Node2t_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benIN = s"${DevSchema}.sep_surety_7M_r4_t2"
    val Nodet_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benOUT = s"${DevSchema}.sep_surety_7M_for_non_client_gr_ben"
    val dashboardPath = s"${config.auxPath}sep_surety_7M_for_non_client_gr_ben"


    override val dashboardName: String = Nodet_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benOUT //витрина
    override def processName: String = "SET"

    def DoSepSurety7MForNonClientGrBen() {

      Logger.getLogger(Nodet_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benOUT).setLevel(Level.WARN)

      logStart()
//количество соседей нигде не используется
      val readHiveTable1 = spark.sql(s"""
select 
    b.u7m_id,
    kr_gr.surety_inn_kr_gr,
    kr_ben.surety_inn_kr_ben,
    db_gr.surety_inn_db_gr,
    db_ben.surety_inn_db_ben
from $Node1t_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benIN as b
left join (
            select u7m_id, surety_inn_kr_gr from $Node2t_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benIN
            where surety_inn_kr_gr is not null 
              and (
                1=1  or ---PARAM: 1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал  
                (
                kr_gr_1q_flag = 1
                 and (
                    (max_oboroty_last_tr_m > 9 and (kr_gr_1q_flag+kr_gr_2q_flag+kr_gr_3q_flag+kr_gr_4q_flag)>=3)
                        or
                    (max_oboroty_last_tr_m > 6 and max_oboroty_last_tr_m <= 9  and (kr_gr_1q_flag+kr_gr_2q_flag+kr_gr_3q_flag) = 3)
                        or
                    (max_oboroty_last_tr_m > 3 and max_oboroty_last_tr_m <= 6  and (kr_gr_1q_flag+kr_gr_2q_flag) = 2)
                        or
                    (max_oboroty_last_tr_m > 0 and max_oboroty_last_tr_m <= 3  and kr_gr_1q_flag = 1)
                    ))
                  )  
                    
) as kr_gr on kr_gr.u7m_id = b.u7m_id

left join 
    (
            select u7m_id, surety_inn_kr_ben 
            from $Node2t_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benIN
            where surety_inn_kr_ben is not null 
              and (  
                 1=1  or ---PARAM: 1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал
                (
                 kr_ben_1q_flag = 1
                and (
                    (max_oboroty_ben_last_tr_m > 9 and (kr_ben_1q_flag+kr_ben_2q_flag+kr_ben_3q_flag+kr_ben_4q_flag)>=3)
                        or
                    (max_oboroty_ben_last_tr_m > 6 and max_oboroty_ben_last_tr_m <= 9  and (kr_ben_1q_flag+kr_ben_2q_flag+kr_ben_3q_flag) = 3)
                        or
                    (max_oboroty_ben_last_tr_m > 3 and max_oboroty_ben_last_tr_m <= 6  and (kr_ben_1q_flag+kr_ben_2q_flag) = 2)
                        or
                    (max_oboroty_ben_last_tr_m > 0 and max_oboroty_ben_last_tr_m <= 3  and kr_ben_1q_flag = 1)
                    )
                 )
                 )   

    ) as kr_ben 
    on kr_ben.u7m_id = b.u7m_id
left join (
            select u7m_id, surety_inn_db_gr 
            from $Node2t_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benIN
            where surety_inn_db_gr is not null
            and (  
                 1=1  or ---PARAM: 1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал
                (                          
                 db_gr_1q_flag = 1 
                and (
                    (max_oboroty_last_tr_m > 9 and (db_gr_1q_flag+db_gr_2q_flag+db_gr_3q_flag+db_gr_4q_flag)>=3)
                        or
                    (max_oboroty_last_tr_m > 6 and max_oboroty_last_tr_m <= 9  and (db_gr_1q_flag+db_gr_2q_flag+db_gr_3q_flag) = 3)
                        or
                    (max_oboroty_last_tr_m > 3 and max_oboroty_last_tr_m <= 6  and (db_gr_1q_flag+db_gr_2q_flag) = 2)
                        or
                    (max_oboroty_last_tr_m > 0 and max_oboroty_last_tr_m <= 3  and db_gr_1q_flag = 1)
                    )
                   )) 
                    
) as db_gr on db_gr.u7m_id = b.u7m_id

left join (
            select u7m_id, surety_inn_db_ben 
            from $Node2t_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benIN
            where surety_inn_db_ben is not null
            and (  
                 1=1  or ---PARAM: 1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал
                (
                db_ben_1q_flag = 1 
                and (
                    (max_oboroty_ben_last_tr_m > 9 and (db_ben_1q_flag+db_ben_2q_flag+db_ben_3q_flag+db_ben_4q_flag)>=3)
                        or
                    (max_oboroty_ben_last_tr_m > 6 and max_oboroty_ben_last_tr_m <= 9  and (db_ben_1q_flag+db_ben_2q_flag+db_ben_3q_flag) = 3)
                        or
                    (max_oboroty_ben_last_tr_m > 3 and max_oboroty_ben_last_tr_m <= 6  and (db_ben_1q_flag+db_ben_2q_flag) = 2)
                        or
                    (max_oboroty_ben_last_tr_m > 0 and max_oboroty_ben_last_tr_m <= 3  and db_ben_1q_flag = 1)
                    )
                   ))   

) as db_ben 
on db_ben.u7m_id = b.u7m_id
where b.flag_basis_client = 'Y'   
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_sep_surety_7M_for_non_client_gr_benOUT")

      logInserted()
      logEnd()
    }
}

