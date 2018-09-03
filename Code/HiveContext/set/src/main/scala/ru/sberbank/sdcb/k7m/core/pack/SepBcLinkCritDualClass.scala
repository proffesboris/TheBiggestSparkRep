package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SepBcLinkCritDualClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_sep_bc_link_crit_dualIN = s"${MartSchema}.clu"
    val Node2t_team_k7m_aux_d_sep_bc_link_crit_dualIN = s"${DevSchema}.sep_link_crit_dual"
    val Nodet_team_k7m_aux_d_sep_bc_link_crit_dualOUT = s"${DevSchema}.sep_bc_link_crit_dual"
    val dashboardPath = s"${config.auxPath}sep_bc_link_crit_dual"


    override val dashboardName: String = Nodet_team_k7m_aux_d_sep_bc_link_crit_dualOUT //витрина
    override def processName: String = "SET"

    def DoSetsep_bc_link_crit_dual() {

      Logger.getLogger("sep_bc_link_crit_dual").setLevel(Level.WARN)
      //STEP 5 Проведем отсев проводок и отсеем договора так, чтобы существовали транзакции выдачи или гашения по ОД

      //отберем транзакции гашения
      logStart()

      //Подготовка связей 2
      val readHiveTable1 = spark.sql(s"""
select 
    d.bor_obj,
    d.bor_u7m_id, 
    b.inn bor_inn,
    d.rel_obj,
    d.rel_u7m_id,
    r.inn rel_inn,
    coalesce(d.ben_flag,0) as ben_flag  
from  $Node1t_team_k7m_aux_d_sep_bc_link_crit_dualIN b
join $Node2t_team_k7m_aux_d_sep_bc_link_crit_dualIN d
on b.u7m_id = d.bor_u7m_id
join $Node1t_team_k7m_aux_d_sep_bc_link_crit_dualIN r
on r.u7m_id = d.rel_u7m_id
where b.flag_basis_client = 'Y'
    and d.bor_obj = 'CLU'
    and d.rel_obj = 'CLU'
""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_sep_bc_link_crit_dualOUT")


      logInserted()
      logEnd()
    }
}

