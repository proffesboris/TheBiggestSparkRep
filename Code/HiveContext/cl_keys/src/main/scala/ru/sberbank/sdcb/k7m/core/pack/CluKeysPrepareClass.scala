package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class CluKeysPrepareClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_clu_keys_prepareIN = s"${DevSchema}.clu_keys_core"
    val Nodet_team_k7m_aux_d_clu_keys_prepareOUT = s"${DevSchema}.clu_keys_prepare"
    val dashboardPath = s"${config.auxPath}clu_keys_prepare"


    override val dashboardName: String = Nodet_team_k7m_aux_d_clu_keys_prepareOUT //витрина
    override def processName: String = "clu_keys"

    def DoCluKeysPrepare() {

      Logger.getLogger(Nodet_team_k7m_aux_d_clu_keys_prepareOUT).setLevel(Level.WARN)

      logStart()


      //Отдельный этап. Выполняется ПЕРЕД ключами и служит основой для ключей.
      //Эк.связи. Пока только CLU
      val createHiveTableStage1 = spark.sql(
    s"""select
    c.crm_id,
    c.inn,
    c.lvl,
    row_number() over (partition by c.crm_id order by c.lvl, c.inn)  crm_dup_no,
    row_number() over (partition by c.inn order by c.lvl, c.crm_id) inn_dup_no
from $Node1t_team_k7m_aux_d_clu_keys_prepareIN c
    """).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_prepareOUT")

      logInserted()

      //---------------------------------------------------------------------
      //--  Выполнить проверки:
      //  -- Записать расхождения в журнал CUSTOM_LOG с типом ERR
      //---------------------------------------------------------------------

      //1. Противоречия crm_id и ИНН в поступившем наборе данных. Если есть записи - записать в CUSTOM_LOG с ошибкой
      val checkStage1 = spark.sql(s"""
    select cast(count(*) as string) cnt
      from ${config.aux}.clu_keys_prepare x
        where x.crm_dup_no>1 and x.crm_id is not null
      or x.inn_dup_no>1 and x.inn is not null""").collect().toSeq

      val log1 = checkStage1.foreach(c=>
        log("clu_keys","Противоречия crm_id и ИНН в поступившем наборе данных. Всего "+c.getString(0), CustomLogStatus.ERROR))

      logEnd()
    }
}

