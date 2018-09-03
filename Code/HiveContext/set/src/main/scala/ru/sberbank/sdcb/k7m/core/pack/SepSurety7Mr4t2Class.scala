package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SepSurety7Mr4t2Class(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa
//TODO исполнить требование прототипа по параметризации:
//  --ВНИМАНИЮ РАЗРАБОТЧИКА
//  --Константа DATE'2018-02-01' - это параметр Дата расчета K7M!
//
//  --Ниже в SQL вводятся 2 параметра
//  -- 1 PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
//  -- 2 PARAM "Попадание в топ по оборотам"
//  -- 3 PARAM: 1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал
//  --Необходимо завести их в  таблице (Борис Медведев создает)
//  create table aux.ETL_TASK_PARAM
//  (
//    Proc_cd      string,
//    param_name string,
//    param_val string,
//    param_desc string)
//  --заполняем ее так:
//    --1)PARAM
//  -- Proc_cd       = SET
//  -- param_name    = TURNOVER_PART
//  --param_val =      0.025
//  --param_desc = "Доля оборотов потенц. поручителя в общих оборотах контрагента"
//  --2)PARAM
//  -- Proc_cd       = SET
//  -- param_name    = TURNOVER_TOP
//  --param_val =      10
//  --param_desc = "Попадание в топ по оборотам"
//  --3)PARAM
//  -- Proc_cd       = SET
//  -- param_name    = TURN_OFF_Q_STABILITY
//  -- param_val     = 1
//  -- param_desc = "1 - Не использовать логику анализа стабильности оборотов за квартал, 0 - Использовать логику анализа стабильности оборотов за квартал"
//  ---Далее в коде заменить хардкод на использование этих параметров, обратите внимание, что параметры имеют тип string и их надо преобразовывать к double или int для сравнения!!



  val Node1t_team_k7m_aux_d_sep_surety_7M_r4_t2IN = s"${MartSchema}.clu"
    val Node2t_team_k7m_aux_d_sep_surety_7M_r4_t2IN = s"${DevSchema}.sep_surety_7M_r4_oboroty_ord"
    val Node5t_team_k7m_aux_d_sep_surety_7M_r4_t2IN = s"${DevSchema}.sep_bc_link_crit_dual"
    val Nodet_team_k7m_aux_d_sep_surety_7M_r4_t2OUT = s"${DevSchema}.sep_surety_7M_r4_t2"
    val dashboardPath = s"${config.auxPath}sep_surety_7M_r4_t2"


    override val dashboardName: String = Nodet_team_k7m_aux_d_sep_surety_7M_r4_t2OUT //витрина
    override def processName: String = "SET"

    def DoSepSurety7Mr4t2() {

      Logger.getLogger(Nodet_team_k7m_aux_d_sep_surety_7M_r4_t2OUT).setLevel(Level.WARN)

      logStart()
//количество соседей нигде не используется
      val readHiveTable1 = spark.sql(s"""
select a.u7m_id,
 kr.oboroty_kr,kr.oboroty_kr_1q
,kr.oboroty_kr_2q
,kr.oboroty_kr_3q
,kr.oboroty_kr_4q
,kr_ben.oboroty_kr as kr_ben_oboroty_kr
,kr_ben.oboroty_kr_1q as kr_ben_oboroty_kr_1q
,kr_ben.oboroty_kr_2q as kr_ben_oboroty_kr_2q
,kr_ben.oboroty_kr_3q as kr_ben_oboroty_kr_3q
,kr_ben.oboroty_kr_4q as kr_ben_oboroty_kr_4q
,kr.oboroty_kr_gr
,kr.oboroty_kr_1q_gr
,kr.oboroty_kr_2q_gr
,kr.oboroty_kr_3q_gr
,kr.oboroty_kr_4q_gr
,kr_ben.oboroty_kr_ben
,kr_ben.oboroty_kr_1q_ben
,kr_ben.oboroty_kr_2q_ben
,kr_ben.oboroty_kr_3q_ben
,kr_ben.oboroty_kr_4q_ben
,kr.oboroty_kr_gr_rate
,kr.oboroty_kr_1q_gr_rate
,kr.oboroty_kr_2q_gr_rate
,kr.oboroty_kr_3q_gr_rate
,kr.oboroty_kr_4q_gr_rate
,kr_ben.oboroty_kr_ben_rate
,kr_ben.oboroty_kr_1q_ben_rate
,kr_ben.oboroty_kr_2q_ben_rate
,kr_ben.oboroty_kr_3q_ben_rate
,kr_ben.oboroty_kr_4q_ben_rate
,kr.rn_oboroty_kr
,kr.rn_oboroty_kr_1q
,kr.rn_oboroty_kr_2q
,kr.rn_oboroty_kr_3q
,kr.rn_oboroty_kr_4q
,kr.rn_oboroty_kr_gr_rate
,kr.rn_oboroty_kr_1q_gr_rate
,kr.rn_oboroty_kr_2q_gr_rate
,kr.rn_oboroty_kr_3q_gr_rate
,kr.rn_oboroty_kr_4q_gr_rate
,kr_ben.rn_oboroty_kr_ben_rate
,kr_ben.rn_oboroty_kr_1q_ben_rate
,kr_ben.rn_oboroty_kr_2q_ben_rate
,kr_ben.rn_oboroty_kr_3q_ben_rate
,kr_ben.rn_oboroty_kr_4q_ben_rate
,db.oboroty_db
,db.oboroty_db_1q
,db.oboroty_db_2q
,db.oboroty_db_3q
,db.oboroty_db_4q
,db_ben.oboroty_db as db_ben_oboroty_db
,db_ben.oboroty_db_1q as db_ben_oboroty_db_1q
,db_ben.oboroty_db_2q as db_ben_oboroty_db_2q
,db_ben.oboroty_db_3q as db_ben_oboroty_db_3q
,db_ben.oboroty_db_4q as db_ben_oboroty_db_4q
,db.oboroty_db_gr
,db.oboroty_db_1q_gr
,db.oboroty_db_2q_gr
,db.oboroty_db_3q_gr
,db.oboroty_db_4q_gr
,db_ben.oboroty_db_ben
,db_ben.oboroty_db_1q_ben
,db_ben.oboroty_db_2q_ben
,db_ben.oboroty_db_3q_ben
,db_ben.oboroty_db_4q_ben
,db.oboroty_db_gr_rate
,db.oboroty_db_1q_gr_rate
,db.oboroty_db_2q_gr_rate
,db.oboroty_db_3q_gr_rate
,db.oboroty_db_4q_gr_rate
,db_ben.oboroty_db_ben_rate
,db_ben.oboroty_db_1q_ben_rate
,db_ben.oboroty_db_2q_ben_rate
,db_ben.oboroty_db_3q_ben_rate
,db_ben.oboroty_db_4q_ben_rate
,db.rn_oboroty_db
,db.rn_oboroty_db_1q
,db.rn_oboroty_db_2q
,db.rn_oboroty_db_3q
,db.rn_oboroty_db_4q
,db.rn_oboroty_db_gr_rate
,db.rn_oboroty_db_1q_gr_rate
,db.rn_oboroty_db_2q_gr_rate
,db.rn_oboroty_db_3q_gr_rate
,db.rn_oboroty_db_4q_gr_rate
,db_ben.rn_oboroty_db_ben_rate
,db_ben.rn_oboroty_db_1q_ben_rate
,db_ben.rn_oboroty_db_2q_ben_rate
,db_ben.rn_oboroty_db_3q_ben_rate
,db_ben.rn_oboroty_db_4q_ben_rate,
case when kr.oboroty_kr_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr.rn_oboroty_kr <= 10 -- PARAM "Попадание в топ по оборотам"
     then kr.rel_inn end as surety_inn_kr_gr
    ,case when kr.oboroty_kr_1q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr.rn_oboroty_kr_1q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr.rel_inn is not NULL
     then 1 else 0 end as kr_gr_1q_flag
    ,case when kr.oboroty_kr_2q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr.rn_oboroty_kr_2q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr.rel_inn is not NULL
     then 1 else 0 end as kr_gr_2q_flag
    ,case when kr.oboroty_kr_3q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr.rn_oboroty_kr_3q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr.rel_inn is not NULL
     then 1 else 0 end as kr_gr_3q_flag
    ,case when kr.oboroty_kr_4q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr.rn_oboroty_kr_4q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr.rel_inn is not NULL
     then 1 else 0 end as kr_gr_4q_flag
    ,case when kr_ben.oboroty_kr_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr_ben.rn_oboroty_kr <= 10 -- PARAM "Попадание в топ по оборотам"
     then kr_ben.rel_inn end as surety_inn_kr_ben
    ,case when kr_ben.oboroty_kr_1q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr_ben.rn_oboroty_kr_1q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr_ben.rel_inn is not NULL
     then 1 else 0 end as kr_ben_1q_flag
    ,case when kr_ben.oboroty_kr_2q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr_ben.rn_oboroty_kr_2q <= 10  -- PARAM "Попадание в топ по оборотам"
           and kr_ben.rel_inn is not NULL
     then 1 else 0 end as kr_ben_2q_flag
    ,case when kr_ben.oboroty_kr_3q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr_ben.rn_oboroty_kr_3q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr_ben.rel_inn is not NULL
     then 1 else 0 end as kr_ben_3q_flag
    ,case when kr_ben.oboroty_kr_4q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and kr_ben.rn_oboroty_kr_4q <= 10 -- PARAM "Попадание в топ по оборотам"
           and kr_ben.rel_inn is not NULL
     then 1 else 0 end as kr_ben_4q_flag
    ,case when db.oboroty_db_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db.rn_oboroty_db <= 10 -- PARAM "Попадание в топ по оборотам"
     then db.rel_inn end as surety_inn_db_gr
    ,case when db.oboroty_db_1q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db.rn_oboroty_db_1q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db.rel_inn is not NULL
     then 1 else 0 end as db_gr_1q_flag
    ,case when db.oboroty_db_2q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db.rn_oboroty_db_2q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db.rel_inn is not NULL
     then 1 else 0 end as db_gr_2q_flag
    ,case when db.oboroty_db_3q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db.rn_oboroty_db_3q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db.rel_inn is not NULL
     then 1 else 0 end as db_gr_3q_flag
    ,case when db.oboroty_db_4q_gr_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db.rn_oboroty_db_4q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db.rel_inn is not NULL
     then 1 else 0 end as db_gr_4q_flag,
     case when db_ben.oboroty_db_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db_ben.rn_oboroty_db <= 10 -- PARAM "Попадание в топ по оборотам"
     then db_ben.rel_inn end as surety_inn_db_ben,
     case when db_ben.oboroty_db_1q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db_ben.rn_oboroty_db_1q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db_ben.rel_inn is not NULL
     then 1 else 0 end as db_ben_1q_flag,
     case when db_ben.oboroty_db_2q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db_ben.rn_oboroty_db_2q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db_ben.rel_inn is not NULL
     then 1 else 0 end as db_ben_2q_flag,
     case when db_ben.oboroty_db_3q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db_ben.rn_oboroty_db_3q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db_ben.rel_inn is not NULL
     then 1 else 0 end as db_ben_3q_flag
    ,case when db_ben.oboroty_db_4q_ben_rate > 0.025 -- PARAM "Доля оборотов потенц. поручителя в общих оборотах контрагента"
           and db_ben.rn_oboroty_db_4q <= 10 -- PARAM "Попадание в топ по оборотам"
           and db_ben.rel_inn is not NULL
     then 1 else 0 end as db_ben_4q_flag,
     kr.max_oboroty_kr_last_tr_m,
     db.max_oboroty_db_last_tr_m,
     kr.max_oboroty_last_tr_m,
     kr_ben.max_oboroty_kr_last_tr_m as max_oboroty_kr_ben_last_tr_m,
     db_ben.max_oboroty_db_last_tr_m as max_oboroty_db_ben_last_tr_m,
     kr_ben.max_oboroty_last_tr_m as max_oboroty_ben_last_tr_m
from $Node1t_team_k7m_aux_d_sep_surety_7M_r4_t2IN as a
left join $Node2t_team_k7m_aux_d_sep_surety_7M_r4_t2IN as kr
on
    A.u7m_id=kr.u7m_id
    and kr.rn_oboroty_kr_gr_rate = 1
left join $Node2t_team_k7m_aux_d_sep_surety_7M_r4_t2IN as kr_ben
on
    A.u7m_id=kr_ben.u7m_id
    and kr_ben.rn_oboroty_kr_ben_rate = 1
left join $Node2t_team_k7m_aux_d_sep_surety_7M_r4_t2IN as db
on
    A.u7m_id=db.u7m_id
    and db.rn_oboroty_db_gr_rate = 1
left join $Node2t_team_k7m_aux_d_sep_surety_7M_r4_t2IN as db_ben
    on
        A.u7m_id=db_ben.u7m_id
        and db_ben.rn_oboroty_db_ben_rate = 1
where a.flag_basis_client = 'Y'
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_sep_surety_7M_r4_t2OUT")

      logInserted()
      logEnd()
    }
}

