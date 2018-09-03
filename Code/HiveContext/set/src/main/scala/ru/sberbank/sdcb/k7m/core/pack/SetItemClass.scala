package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetItemClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_itemIN = s"${DevSchema}.set"
    val Node2t_team_k7m_aux_d_set_itemIN = s"${DevSchema}.set_set_item"
    val Nodet_team_k7m_aux_d_set_itemOUT = s"${DevSchema}.set_item"
    val dashboardPath = s"${config.auxPath}set_item"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_itemOUT //витрина
    override def processName: String = "SET"

    def DoSetItem() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_itemOUT).setLevel(Level.WARN)

      logStart()
      //добавим поручителей базису
      val readHiveTable1 = spark.sql(s"""
select
  cast(i.set_item_id as string) set_item_id,
  cast(s.set_id as string) set_id,
  i.sur_obj as obj,
  i.sur_u7m_id as u7m_id,
  '$execId' exec_id --Получить из системы логирования
from
    $Node1t_team_k7m_aux_d_set_itemIN s
    join
    $Node2t_team_k7m_aux_d_set_itemIN i
    on s.u7m_b_id = i.u7m_id
       and s.instrument = i.instrument
where set_item_status = 'Ok'
    and sur_u7m_id is not null """)
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_itemOUT")


      logInserted()
      logEnd()
    }
}

