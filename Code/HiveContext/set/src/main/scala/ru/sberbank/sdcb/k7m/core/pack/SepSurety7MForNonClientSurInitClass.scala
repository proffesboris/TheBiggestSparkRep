package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SepSurety7MForNonClientSurInitClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_sep_surety_7M_for_non_client_sur_initIN = s"${DevSchema}.sep_surety_7M_for_non_client_gr_ben"
    val Nodet_team_k7m_aux_d_sep_surety_7M_for_non_client_sur_initOUT = s"${DevSchema}.sep_surety_7M_for_non_client_sur_init"
    val dashboardPath = s"${config.auxPath}sep_surety_7M_for_non_client_sur_init"


    override val dashboardName: String = Nodet_team_k7m_aux_d_sep_surety_7M_for_non_client_sur_initOUT //витрина
    override def processName: String = "SET"

    def DoSepSurety7MForNonClientSurInit() {

      Logger.getLogger(Nodet_team_k7m_aux_d_sep_surety_7M_for_non_client_sur_initOUT).setLevel(Level.WARN)

      logStart()
//количество соседей нигде не используется
      val readHiveTable1 = spark.sql(s"""
select distinct u7m_id, sur_inn, ben_flag from (
    select u7m_id
          ,coalesce(surety_inn_kr_ben,surety_inn_kr_gr) as sur_inn
          ,case when surety_inn_kr_ben is not null then 1 else 0 end as ben_flag
    from $Node1t_team_k7m_aux_d_sep_surety_7M_for_non_client_sur_initIN
    union all
    select u7m_id,
        coalesce(surety_inn_db_ben,surety_inn_db_gr) as sur_inn,
        case when surety_inn_db_ben is not null then 1 else 0 end as ben_flag
    from $Node1t_team_k7m_aux_d_sep_surety_7M_for_non_client_sur_initIN
) x
where sur_inn is not null  
        """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_sep_surety_7M_for_non_client_sur_initOUT")

      logInserted()
      logEnd()
    }
}

