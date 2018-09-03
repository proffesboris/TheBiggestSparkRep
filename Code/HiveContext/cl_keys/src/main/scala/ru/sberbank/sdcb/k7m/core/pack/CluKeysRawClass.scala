package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class CluKeysRawClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {


    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_clu_keys_rawIN = s"${DevSchema}.lk_lkc_raw"
    val Node2t_team_k7m_aux_d_clu_keys_rawIN = s"${DevSchema}.basis_client"
    val Nodet_team_k7m_aux_d_clu_keys_rawOUT = s"${DevSchema}.clu_keys_raw"
    val dashboardPath = s"${config.auxPath}clu_keys_raw"


    override val dashboardName: String = Nodet_team_k7m_aux_d_clu_keys_rawOUT //витрина
    override def processName: String = "clu_keys"

    def DoCluKeysRaw() {

      Logger.getLogger(Nodet_team_k7m_aux_d_clu_keys_rawOUT).setLevel(Level.WARN)

logStart()

      //Отдельный этап. Выполняется ПЕРЕД ключами и служит основой для ключей.
      //Эк.связи. Пока только CLU
      val createHiveTableStage1 = spark.sql(
    s"""select
            distinct
    inn,
    case when  cnt>1
    then
        cast (null as string)
    else crm_id
    end crm_id
from
(
select
    max(crm_id) as crm_id ,
    max(inn) as inn ,
    count(inn) cnt
from (
select
    crm_id,
    inn
from (
select
    id_from crm_id,
    inn_from inn
from $Node1t_team_k7m_aux_d_clu_keys_rawIN
where t_from = 'CLU'
union ALL
select
    id_to crm_id,
    inn_to inn
from $Node1t_team_k7m_aux_d_clu_keys_rawIN
where t_to = 'CLU'
union ALL
select
    org_crm_id,
    org_inn_crm_num inn
from $Node2t_team_k7m_aux_d_clu_keys_rawIN
) x
where
     (crm_id is not null or
     inn is not null
     AND length(inn) in (10,12)
     and inn not like '00%'
     )
group by
    crm_id,
    inn
    )  y
group by coalesce(inn, crm_id)
) z
    """).write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_keys_rawOUT")

      logInserted()

      //---------------------------------------------------------------------
      //--Постусловия на ключи:
      //-- Если один  2-х запросов ниже возвращает записи - писать ошибку в CUSTOM_LOG и падать с  ошибкой! РАСЧЕТ ПРОДОЛЖАТЬ НЕЛЬЗЯ
      //---------------------------------------------------------------------

      //1.
      val checkStage1 = spark.sql(s"""
    select inn,
       count(*)
from $Nodet_team_k7m_aux_d_clu_keys_rawOUT
where crm_id is not null
    and inn is  not null
group  by inn
having count(*) > 1 """).collect().toSeq

      val log1 = checkStage1.foreach(c=>{
        log("clu_keys","INN. "+c.getString(0)+" " + c.getString(1)+ ": " +c.getString(2), CustomLogStatus.ERROR)
      })

      //2.
      val checkStage2 = spark.sql(s"""
   select crm_id,
       count(*)
from $Nodet_team_k7m_aux_d_clu_keys_rawOUT
where crm_id is not null
    and inn is  not null
group  by crm_id
having count(*) > 1""").collect().toSeq

      val log2 = checkStage2.foreach(c=> {
        log("clu_keys", "CRM_ID. " + c.getString(0) + ": " + c.getString(1), CustomLogStatus.ERROR)
      })

      val flagEx: Boolean = !checkStage1.isEmpty || !checkStage2.isEmpty

      if (flagEx) throw new IllegalArgumentException
      (s"${if (!checkStage1.isEmpty) "Дубли inn."} ${if (!checkStage2.isEmpty) "Дубли crm_id."}")

logEnd()
    }
}

