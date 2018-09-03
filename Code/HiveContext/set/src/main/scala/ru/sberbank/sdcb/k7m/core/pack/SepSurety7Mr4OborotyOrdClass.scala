package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SepSurety7Mr4OborotyOrdClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_sep_surety_7M_r4_oboroty_ordIN = s"${DevSchema}.sep_surety_7M_r4_gr_ben"
    val Nodet_team_k7m_aux_d_sep_surety_7M_r4_oboroty_ordOUT = s"${DevSchema}.sep_surety_7M_r4_oboroty_ord"
    val dashboardPath = s"${config.auxPath}sep_surety_7M_r4_oboroty_ord"


    override val dashboardName: String = Nodet_team_k7m_aux_d_sep_surety_7M_r4_oboroty_ordOUT //витрина
    override def processName: String = "SET"

    def DoSepSurety7Mr4OborotyOrd() {

      Logger.getLogger(Nodet_team_k7m_aux_d_sep_surety_7M_r4_oboroty_ordOUT).setLevel(Level.WARN)

      logStart()
//количество соседей нигде не используется
      val readHiveTable1 = spark.sql(s"""
select 
    a.u7m_id,
    a.rel_inn,
    a.rel_u7m_id,
    a.rel_flag,
    a.ben_flag,
    a.oboroty_kr   ,a.oboroty_kr_1q   ,a.oboroty_kr_2q   ,a.oboroty_kr_3q   ,a.oboroty_kr_4q,
    a.oboroty_kr_gr,a.oboroty_kr_1q_gr,a.oboroty_kr_2q_gr,a.oboroty_kr_3q_gr,a.oboroty_kr_4q_gr,
    a.oboroty_kr_ben,a.oboroty_kr_1q_ben,a.oboroty_kr_2q_ben,a.oboroty_kr_3q_ben,a.oboroty_kr_4q_ben
    ,a.oboroty_kr_gr   /a.total_oboroty_kr    as oboroty_kr_gr_rate
    ,a.oboroty_kr_1q_gr/a.total_oboroty_kr_1q as oboroty_kr_1q_gr_rate
    ,a.oboroty_kr_2q_gr/a.total_oboroty_kr_2q as oboroty_kr_2q_gr_rate
    ,a.oboroty_kr_3q_gr/a.total_oboroty_kr_3q as oboroty_kr_3q_gr_rate
    ,a.oboroty_kr_4q_gr/a.total_oboroty_kr_4q as oboroty_kr_4q_gr_rate
    ,a.oboroty_kr_ben   /a.total_oboroty_kr    as oboroty_kr_ben_rate
    ,a.oboroty_kr_1q_ben/a.total_oboroty_kr_1q as oboroty_kr_1q_ben_rate
    ,a.oboroty_kr_2q_ben/a.total_oboroty_kr_2q as oboroty_kr_2q_ben_rate
    ,a.oboroty_kr_3q_ben/a.total_oboroty_kr_3q as oboroty_kr_3q_ben_rate
    ,a.oboroty_kr_4q_ben/a.total_oboroty_kr_4q as oboroty_kr_4q_ben_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr desc)     as rn_oboroty_kr,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_1q desc)  as rn_oboroty_kr_1q,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_2q desc)  as rn_oboroty_kr_2q,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_3q desc)  as rn_oboroty_kr_3q,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_4q desc)  as rn_oboroty_kr_4q,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_gr/a.total_oboroty_kr       desc) as rn_oboroty_kr_gr_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_1q_gr/a.total_oboroty_kr_1q desc) as rn_oboroty_kr_1q_gr_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_2q_gr/a.total_oboroty_kr_2q desc) as rn_oboroty_kr_2q_gr_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_3q_gr/a.total_oboroty_kr_3q desc) as rn_oboroty_kr_3q_gr_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_4q_gr/a.total_oboroty_kr_4q desc) as rn_oboroty_kr_4q_gr_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_ben/a.total_oboroty_kr       desc) as rn_oboroty_kr_ben_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_1q_ben/a.total_oboroty_kr_1q desc) as rn_oboroty_kr_1q_ben_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_2q_ben/a.total_oboroty_kr_2q desc) as rn_oboroty_kr_2q_ben_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_3q_ben/a.total_oboroty_kr_3q desc) as rn_oboroty_kr_3q_ben_rate,
    row_number() over (partition by a.u7m_id order by a.oboroty_kr_4q_ben/a.total_oboroty_kr_4q desc) as rn_oboroty_kr_4q_ben_rate,
    a.oboroty_db   ,a.oboroty_db_1q   ,a.oboroty_db_2q   ,a.oboroty_db_3q   ,a.oboroty_db_4q,
    a.oboroty_db_gr,a.oboroty_db_1q_gr,a.oboroty_db_2q_gr,a.oboroty_db_3q_gr,a.oboroty_db_4q_gr,
    a.oboroty_db_ben,a.oboroty_db_1q_ben,a.oboroty_db_2q_ben,a.oboroty_db_3q_ben,a.oboroty_db_4q_ben,
    a.oboroty_db_gr   /a.total_oboroty_db    as oboroty_db_gr_rate,
    a.oboroty_db_1q_gr/a.total_oboroty_db_1q as oboroty_db_1q_gr_rate,
    a.oboroty_db_2q_gr/a.total_oboroty_db_2q as oboroty_db_2q_gr_rate,
    a.oboroty_db_3q_gr/a.total_oboroty_db_3q as oboroty_db_3q_gr_rate,
    a.oboroty_db_4q_gr/a.total_oboroty_db_4q as oboroty_db_4q_gr_rate,
    a.oboroty_db_ben   /a.total_oboroty_db    as oboroty_db_ben_rate,
    a.oboroty_db_1q_ben/a.total_oboroty_db_1q as oboroty_db_1q_ben_rate,
    a.oboroty_db_2q_ben/a.total_oboroty_db_2q as oboroty_db_2q_ben_rate,
    a.oboroty_db_3q_ben/a.total_oboroty_db_3q as oboroty_db_3q_ben_rate,
    a.oboroty_db_4q_ben/a.total_oboroty_db_4q as oboroty_db_4q_ben_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db desc)     as rn_oboroty_db,
    row_number() over (partition by  a.u7m_id order by a.oboroty_db_1q desc)  as rn_oboroty_db_1q,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_2q desc)  as rn_oboroty_db_2q,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_3q desc)  as rn_oboroty_db_3q,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_4q desc)  as rn_oboroty_db_4q,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_gr/a.total_oboroty_db desc) as rn_oboroty_db_gr_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_1q_gr/a.total_oboroty_db_1q desc) as rn_oboroty_db_1q_gr_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_2q_gr/a.total_oboroty_db_2q desc) as rn_oboroty_db_2q_gr_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_3q_gr/a.total_oboroty_db_3q desc) as rn_oboroty_db_3q_gr_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_4q_gr/a.total_oboroty_db_4q desc) as rn_oboroty_db_4q_gr_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_ben/a.total_oboroty_db desc) as rn_oboroty_db_ben_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_1q_ben/a.total_oboroty_db_1q desc) as rn_oboroty_db_1q_ben_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_2q_ben/a.total_oboroty_db_2q desc) as rn_oboroty_db_2q_ben_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_3q_ben/a.total_oboroty_db_3q desc) as rn_oboroty_db_3q_ben_rate,
    row_number() over (partition by  a.u7m_id  order by a.oboroty_db_4q_ben/a.total_oboroty_db_4q desc) as rn_oboroty_db_4q_ben_rate,
    max_oboroty_kr_last_tr_m,
    max_oboroty_db_last_tr_m,
    max_oboroty_last_tr_m
from $Node1t_team_k7m_aux_d_sep_surety_7M_r4_oboroty_ordIN a
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_sep_surety_7M_r4_oboroty_ordOUT")

      logInserted()
      logEnd()
    }
}

