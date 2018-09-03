package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SepBasisAllSumTrClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_sep_basis_all_sum_trIN = s"${DevSchema}.sep_surety_7M_r4_basis_client"
    val Nodet_team_k7m_aux_d_sep_basis_all_sum_trOUT = s"${DevSchema}.sep_basis_all_sum_tr"
    val dashboardPath = s"${config.auxPath}sep_basis_all_sum_tr"


    override val dashboardName: String = Nodet_team_k7m_aux_d_sep_basis_all_sum_trOUT //витрина
    override def processName: String = "SET"

    def DoSepBasisAllSumTr() {

      Logger.getLogger(Nodet_team_k7m_aux_d_sep_basis_all_sum_trOUT).setLevel(Level.WARN)

      logStart()
//количество соседей нигде не используется
      val readHiveTable1 = spark.sql(s"""
select bor_u7m_id,
    sum(oboroty_kr) as total_oboroty_kr,
    sum(oboroty_kr_1q) as total_oboroty_kr_1q,
    sum(oboroty_kr_2q) as total_oboroty_kr_2q,
    sum(oboroty_kr_3q) as total_oboroty_kr_3q,
    sum(oboroty_kr_4q) as total_oboroty_kr_4q,
    sum(oboroty_db) as total_oboroty_db,
    sum(oboroty_db_1q) as total_oboroty_db_1q,
    sum(oboroty_db_2q) as total_oboroty_db_2q,
    sum(oboroty_db_3q) as total_oboroty_db_3q,
    sum(oboroty_db_4q) as total_oboroty_db_4q,
    max(oboroty_kr_last_tr_m) as max_oboroty_kr_last_tr_m,
    max(oboroty_db_last_tr_m) as max_oboroty_db_last_tr_m,
    max(oboroty_last_tr_m) as max_oboroty_last_tr_m
from $Node1t_team_k7m_aux_d_sep_basis_all_sum_trIN
group by bor_u7m_id
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_sep_basis_all_sum_trOUT")

      logInserted()
      logEnd()
    }
}

