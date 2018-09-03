package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetSuretyHeritageDogSurCntClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_surety_heritage_dog_sur_cntIN = s"${DevSchema}.set_surety_heritage_dog_sur"
    val  Nodet_team_k7m_aux_d_set_surety_heritage_dog_sur_cntOUT = s"${DevSchema}.set_surety_heritage_dog_sur_cnt"
    val dashboardPath = s"${config.auxPath}set_surety_heritage_dog_sur_cnt"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_surety_heritage_dog_sur_cntOUT //витрина
    override def processName: String = "SET"

    def DoSetSuretyHeritageDogSurCnt() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_surety_heritage_dog_sur_cntOUT).setLevel(Level.WARN)

      logStart()
      //добавим кол-во поручителей
      val readHiveTable1 = spark.sql(s"""
--добавим кол-во поручителей
select
    t1.u7m_id,
    t1.inn,
    t1.org_short_crm_name,
    t1.org_segment_name,
    t1.instrument,
    t1.heritance_flag,
    t1.sur_dog_type,
    t1.pr_cred_id,
    t1.sc_pr_cred_id,
    t1.w_pr_cred_id,
    t1.ndog,
    t1.ddog,
    t1.sur_obj,
    t1.sur_f7m_id,
    t1.sur_inn,
    t1.loc_id,
    case when cnt.cnt_sur > 10 then 1 else 0 end as too_much_sur_flag,
    cnt.cnt_sur
from $Node1t_team_k7m_aux_d_set_surety_heritage_dog_sur_cntIN t1
left join (
    select pr_cred_id, instrument, sum(case when sur_f7m_id is not null or sur_inn is not null then 1 else 0 end) as cnt_sur
    from $Node1t_team_k7m_aux_d_set_surety_heritage_dog_sur_cntIN
    group by pr_cred_id, instrument
    ) cnt
on
    t1.pr_cred_id = cnt.pr_cred_id
    and t1.instrument = cnt.instrument """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_surety_heritage_dog_sur_cntOUT")


      logInserted()
      logEnd()
    }
}

