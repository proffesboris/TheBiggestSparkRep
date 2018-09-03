package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SepSurety7Mr4GrBenClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_sep_surety_7M_r4_gr_benIN = s"${DevSchema}.sep_surety_7M_r4_basis_client"
    val Node2t_team_k7m_aux_d_sep_surety_7M_r4_gr_benIN = s"${DevSchema}.sep_basis_all_sum_tr"
    val Node3t_team_k7m_aux_d_sep_surety_7M_r4_gr_benIN = s"${DevSchema}.sep_bc_link_crit_dual"
    val Nodet_team_k7m_aux_d_sep_surety_7M_r4_gr_benOUT = s"${DevSchema}.sep_surety_7M_r4_gr_ben"
    val dashboardPath = s"${config.auxPath}sep_surety_7M_r4_gr_ben"


    override val dashboardName: String = Nodet_team_k7m_aux_d_sep_surety_7M_r4_gr_benOUT //витрина
    override def processName: String = "SET"

    def DoSepSurety7Mr4GrBen() {

      Logger.getLogger(Nodet_team_k7m_aux_d_sep_surety_7M_r4_gr_benOUT).setLevel(Level.WARN)

      logStart()
//количество соседей нигде не используется
      val readHiveTable1 = spark.sql(s"""
select
    c.bor_u7m_id as u7m_id,
    c.rel_inn,
    A.rel_inn a_rel_inn,
    a.rel_u7m_id,
    oboroty_kr,
    oboroty_kr_1q,
    oboroty_kr_2q,
    oboroty_kr_3q,
    oboroty_kr_4q,
    oboroty_db,
    oboroty_db_1q,
    oboroty_db_2q,
    oboroty_db_3q,
    oboroty_db_4q,
    case when A.rel_inn is not NULL then 1 else 0 end as rel_flag,
    coalesce(A.ben_flag,0) as ben_flag,
    d.total_oboroty_kr,
    d.total_oboroty_kr_1q,
    d.total_oboroty_kr_2q,
    d.total_oboroty_kr_3q,
    d.total_oboroty_kr_4q,
    d.total_oboroty_db,
    d.total_oboroty_db_1q,
    d.total_oboroty_db_2q,
    d.total_oboroty_db_3q,
    d.total_oboroty_db_4q,
    case when A.rel_inn is NULL then 0 else c.oboroty_kr    end as oboroty_kr_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_kr_1q end as oboroty_kr_1q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_kr_2q end as oboroty_kr_2q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_kr_3q end as oboroty_kr_3q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_kr_4q end as oboroty_kr_4q_gr,
    case when A.ben_flag = 1 then c.oboroty_kr else 0 end as oboroty_kr_ben,
    case when A.ben_flag = 1 then c.oboroty_kr_1q else 0 end as oboroty_kr_1q_ben,
    case when A.ben_flag = 1 then c.oboroty_kr_2q else 0 end as oboroty_kr_2q_ben,
    case when A.ben_flag = 1 then c.oboroty_kr_3q else 0 end as oboroty_kr_3q_ben,
    case when A.ben_flag = 1 then c.oboroty_kr_4q else 0 end as oboroty_kr_4q_ben,
    case when A.rel_inn is NULL then 0 else c.oboroty_db    end as oboroty_db_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_db_1q end as oboroty_db_1q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_db_2q end as oboroty_db_2q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_db_3q end as oboroty_db_3q_gr,
    case when A.rel_inn is NULL then 0 else c.oboroty_db_4q end as oboroty_db_4q_gr,
    case when A.ben_flag = 1 then c.oboroty_db else 0 end as oboroty_db_ben,
    case when A.ben_flag = 1 then c.oboroty_db_1q else 0 end as oboroty_db_1q_ben,
    case when A.ben_flag = 1 then c.oboroty_db_2q else 0 end as oboroty_db_2q_ben,
    case when A.ben_flag = 1 then c.oboroty_db_3q else 0 end as oboroty_db_3q_ben,
    case when A.ben_flag = 1 then c.oboroty_db_4q else 0 end as oboroty_db_4q_ben,
    d.max_oboroty_kr_last_tr_m,
    d.max_oboroty_db_last_tr_m,
    d.max_oboroty_last_tr_m
  from $Node1t_team_k7m_aux_d_sep_surety_7M_r4_gr_benIN as C
  join $Node2t_team_k7m_aux_d_sep_surety_7M_r4_gr_benIN as D
    on d.bor_u7m_id = c.bor_u7m_id
  left join $Node3t_team_k7m_aux_d_sep_surety_7M_r4_gr_benIN as A
    on A.bor_u7m_id=C.bor_u7m_id
   and A.rel_inn = C.rel_inn
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_sep_surety_7M_r4_gr_benOUT")

      logInserted()
      logEnd()
    }
}

