package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetSetItemClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val Node1t_team_k7m_aux_d_set_set_itemIN = s"${DevSchema}.set_prep_risk"
    val Node2t_team_k7m_aux_d_set_set_itemIN = s"${DevSchema}.sep_cnt_neighbours"
    val Nodet_team_k7m_aux_d_set_set_itemOUT = s"${DevSchema}.set_set_item"
    val dashboardPath = s"${config.auxPath}set_set_item"


    override val dashboardName: String = Nodet_team_k7m_aux_d_set_set_itemOUT //витрина
    override def processName: String = "SET"

    def DoSetSetItem() {

      Logger.getLogger(Nodet_team_k7m_aux_d_set_set_itemOUT).setLevel(Level.WARN)

      logStart()
      //добавим поручителей базису
      val readHiveTable1 = spark.sql(s"""
select
    row_number() over(order by sr.u7m_id, sr.instrument, sr.sur_obj, sr.sur_u7m_id) set_item_id,
    sr.u7m_id,
    sr.instrument,
    sr.sur_class,
    sr.sur_obj,
    sr.sur_u7m_id,
    case
        when sr.sur_class ='SUNCL'
             and surety_status ='FIND'
             and sur_corporate_flag='N'
             then 'Ignore_by_segment'
        when sr.sur_class ='SUNCL'
             and surety_status ='FIND'
        then 'Ok'
        when  sr.sur_class ='SUNCL'
              and sr.sur_rating>21
        then  'Ignore_by_rating'
        else sr.surety_status
     end   set_item_status,
    sr.sur_corporate_flag,
    sr.sur_rating,
    n.cnt_neighbours
from $Node1t_team_k7m_aux_d_set_set_itemIN sr
    left join
   $Node2t_team_k7m_aux_d_set_set_itemIN n
    on sr.u7m_id = n.bor_u7m_id""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$Nodet_team_k7m_aux_d_set_set_itemOUT")


      logInserted()
      logEnd()
    }
}

