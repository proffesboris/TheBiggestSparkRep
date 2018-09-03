package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class CluKeysCoreClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_clu_keys_coreIN = s"${DevSchema}.clu_keys_raw"
    val Node2t_team_k7m_aux_d_clu_keys_coreIN = s"${Stg0Schema}.crm_s_org_ext_x"
    val Node3t_team_k7m_aux_d_clu_keys_coreIN = s"${DevSchema}.k7m_crm_org_major"
    val Nodet_team_k7m_aux_d_clu_keys_coreOUT = s"${DevSchema}.clu_keys_core"
    val dashboardPath = s"${config.auxPath}clu_keys_core"


    override val dashboardName: String = Nodet_team_k7m_aux_d_clu_keys_coreOUT //витрина
    override def processName: String = "clu_keys"

    def DoCluKeysCore() {

      Logger.getLogger(Nodet_team_k7m_aux_d_clu_keys_coreOUT).setLevel(Level.WARN)

      logStart()


      val Nodet_team_k7m_aux_d_clu_keys_coreCRM = s"${DevSchema}.clu_keys_crm_only"
      val dashboardPathCRM = s"${config.auxPath}clu_keys_crm_only"
      val Nodet_team_k7m_aux_d_clu_keys_coreINN = s"${DevSchema}.clu_keys_inn_only"
      val dashboardPathINN = s"${config.auxPath}clu_keys_crm_only"

//1. Полные ключи (те, у которых есть CRM_ID и ИНН)
      val createHiveTableStage1 = spark.sql(
    s"""select
    crm_id,
    inn,
    0 lvl
from $Node1t_team_k7m_aux_d_clu_keys_coreIN
where inn is not null
    and crm_id is not null
    """).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_coreOUT")

 //2. Только CRM_ID
      val createHiveTableStage2 = spark.sql(
    s"""select ce.crm_id
from $Node1t_team_k7m_aux_d_clu_keys_coreIN ce
    left join
    $Nodet_team_k7m_aux_d_clu_keys_coreOUT kc
on kc.crm_id = ce.crm_id
where ce.inn is null
   and ce.crm_id is not null
   and kc.inn is null
    """).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPathCRM)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_coreCRM")

//3. Обогащение ИНН по CRM_ID
      val createHiveTableStage3 = spark.sql(
    s"""select c.crm_id,
       max(m.sbrf_inn) inn,
       1 lvl
from $Nodet_team_k7m_aux_d_clu_keys_coreCRM c
    left join $Node2t_team_k7m_aux_d_clu_keys_coreIN m
    on m.row_id = c.crm_id
group by     c.crm_id
    """).write
        .format("parquet")
        .mode(SaveMode.Append)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_coreOUT")

//4. Только ИНН
      val createHiveTableStage4 = spark.sql(
    s"""select ce.inn
from $Node1t_team_k7m_aux_d_clu_keys_coreIN ce
    left join
    $Nodet_team_k7m_aux_d_clu_keys_coreOUT kc
on kc.inn = ce.inn
where ce.inn is not null
    and ce.crm_id is null
    and kc.crm_id is null
    """).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPathINN)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_coreINN")

//5. Обогащение CRM_ID по ИНН
      val createHiveTableStage5 = spark.sql(
    s"""select
    max(m.id) crm_id,
    c.inn,
    2 lvl
from  $Nodet_team_k7m_aux_d_clu_keys_coreINN c
    left join $Node3t_team_k7m_aux_d_clu_keys_coreIN m
    on m.inn = c.inn
group by     c.inn
    """).write
        .format("parquet")
        .mode(SaveMode.Append)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_coreOUT")

      logInserted()
      logEnd()
    }
}

