package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SepLinkCritDualClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_sep_link_crit_dualIN = s"${Stg0Schema}.rdm_link_criteria_mast"
    val Node2t_team_k7m_aux_d_sep_link_crit_dualIN = s"${MartSchema}.lkc"
    val Node3t_team_k7m_aux_d_sep_link_crit_dualIN = s"${MartSchema}.lk"
    val Nodet_team_k7m_aux_d_sep_link_crit_dualOUT = s"${DevSchema}.sep_link_crit_dual"
    val dashboardPath = s"${config.auxPath}sep_link_crit_dual"


    override val dashboardName: String = Nodet_team_k7m_aux_d_sep_link_crit_dualOUT //витрина
    override def processName: String = "SET"

    def DoSetsep_link_crit_dual() {

      Logger.getLogger("sep_link_crit_dual").setLevel(Level.WARN)
      //STEP 5 Проведем отсев проводок и отсеем договора так, чтобы существовали транзакции выдачи или гашения по ОД

      //отберем транзакции гашения
      logStart()

      //Подготовка связей 1
      val createHiveTable1 = spark.sql(s"""
 select
    bor_obj,
    bor_u7m_id,
    rel_obj,
    rel_u7m_id,    
    max(ben_flag) ben_flag
 from (    
    select 
        lk.t_from bor_obj,
        lk.u7m_id_from bor_u7m_id,
        lk.t_to rel_obj,
        lk.u7m_id_to rel_u7m_id,
        case when lkc.crit_id= '5.1.7ben' then 1 else 0 end as ben_flag
    from $Node1t_team_k7m_aux_d_sep_link_crit_dualIN as c
    join $Node2t_team_k7m_aux_d_sep_link_crit_dualIN as lkc
      on c.code = lkc.crit_id
    join $Node3t_team_k7m_aux_d_sep_link_crit_dualIN as lk
      on lk.lk_id = lkc.lk_id
    where c.set_flag = 1
        and lk.u7m_id_from is not null
        and lk.u7m_id_to is not null
    union all
    select 
        lk.t_to bor_obj,
        lk.u7m_id_to bor_u7m_id,
        lk.t_from rel_obj,
        lk.u7m_id_from rel_u7m_id,
        case when lkc.crit_id= '5.1.7ben' then 1 else 0 end as ben_flag
    from $Node1t_team_k7m_aux_d_sep_link_crit_dualIN as c
    join $Node2t_team_k7m_aux_d_sep_link_crit_dualIN as lkc
      on c.code = lkc.crit_id
    join $Node3t_team_k7m_aux_d_sep_link_crit_dualIN as lk
      on lk.lk_id = lkc.lk_id
    where c.set_flag = 1
        and lk.u7m_id_from is not null
        and lk.u7m_id_from is not null    
    ) x
group by 
    bor_obj,
    bor_u7m_id,
    rel_obj,
    rel_u7m_id
""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_sep_link_crit_dualOUT")


      logInserted()
      logEnd()
    }
}

