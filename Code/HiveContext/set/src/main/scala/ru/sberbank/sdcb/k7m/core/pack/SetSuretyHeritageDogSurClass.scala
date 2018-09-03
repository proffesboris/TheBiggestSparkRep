package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetSuretyHeritageDogSurClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_surety_heritage_dog_surIN = s"${DevSchema}.set_surety_heritage_dog_"
    val Node2t_team_k7m_aux_d_set_surety_heritage_dog_surIN = s"${DevSchema}.exsgen"
    val Node3t_team_k7m_aux_d_set_surety_heritage_dog_surIN = s"${DevSchema}.lk_lkc"
    val  Nodet_team_k7m_aux_d_set_surety_heritage_dog_surOUT = s"${DevSchema}.set_surety_heritage_dog_sur"
    val dashboardPath = s"${config.auxPath}set_surety_heritage_dog_sur"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_surety_heritage_dog_surOUT //витрина
    override def processName: String = "SET"

    def DoSetSuretyHeritageDogSur() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_surety_heritage_dog_surOUT).setLevel(Level.WARN)

      logStart()
      //соберем данные по поручителям (ФПЛ и ПЮЛ)
      val readHiveTable1 = spark.sql(s"""
--соберем данные по поручителям (ФПЛ и ПЮЛ)
select distinct
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
    s.from_obj sur_obj,
    lkc.u7m_id_from as sur_f7m_id,
    s.from_inn as sur_inn,
    s.loc_id
from $Node1t_team_k7m_aux_d_set_surety_heritage_dog_surIN as t1
    inner join $Node2t_team_k7m_aux_d_set_surety_heritage_dog_surIN s
        on s.cred_id= t1.pr_cred_id
    left join $Node3t_team_k7m_aux_d_set_surety_heritage_dog_surIN lkc
        on lkc.link_id= CONCAT('EX_',cast(s.loc_id as string))
            and lkc.t_from = 'CLF'
  where  s.crit_id ='exsgen_set_sur' """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_surety_heritage_dog_surOUT")


      logInserted()
      logEnd()
    }
}

