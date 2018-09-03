package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class CluKeysActiveClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_clu_keys_activeIN = s"${DevSchema}.clu_keys_prepare"
    val Nodet_team_k7m_aux_d_clu_keys_activeOUT = s"${DevSchema}.clu_keys_active"
    val dashboardPath = s"${config.auxPath}clu_keys_active"


    override val dashboardName: String = Nodet_team_k7m_aux_d_clu_keys_activeOUT //витрина
    override def processName: String = "clu_keys"

    def DoCluKeysActive() {

      Logger.getLogger(Nodet_team_k7m_aux_d_clu_keys_activeOUT).setLevel(Level.WARN)

      logStart()


      //Отдельный этап. Выполняется ПЕРЕД ключами и служит основой для ключей.
      //Эк.связи. Пока только CLU
      val createHiveTableStage1 = spark.sql(
    s"""select
    case
        when crm_id is not null
            and crm_dup_no=1
            then  crm_id
            else cast(null as string)
    end crm_id,
    case
       when inn is not null
            and inn_dup_no=1
            then  inn
            else cast(null as string)
    end inn
from $Node1t_team_k7m_aux_d_clu_keys_activeIN
where (crm_dup_no=1 or inn_dup_no=1)
    """).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_activeOUT")

      logInserted()

      //---------------------------------------------------------------------
      //--  Выполнить проверки:
      //  -- Записать расхождения в журнал CUSTOM_LOG с типом ERR
      //---------------------------------------------------------------------

      //Дубли ИНН. Если есть записи - записать в CUSTOM_LOG с ошибкой
      val checkStage1 = spark.sql(s"""
    select cast (sum(cnt) as string) s from
    (select inn,
    cast (count(*) as string) cnt
from ${config.aux}.clu_keys_active
group by inn
having count(*)>1)
        """).collect().toSeq

      val log1 = checkStage1.foreach(c=>
        log("clu_keys","Дубли ИНН. Всего "+c.getString(0), CustomLogStatus.ERROR))

      //Дубли CRM_ID. Если есть записи - записать в CUSTOM_LOG с ошибкой
      val checkStage2 = spark.sql(s"""
     select cast (sum(cnt) as string) s from
    (select crm_id,
    cast (count(*) as string) cnt
from ${config.aux}.clu_keys_active
group by crm_id
having count(*)>1)
        """).collect().toSeq

      val log2 = checkStage2.foreach(c=>
        log("clu_keys","Дубли CRM_ID. Всего "+c.getString(0), CustomLogStatus.ERROR))

      logEnd()
    }
}

