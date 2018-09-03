package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class LKLKCClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {
  import LkMainClass._
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_lk_lkcIN = s"${DevSchema}.lk_lkc_with_keys"
  val Nodet_team_k7m_aux_d_lk_lkcOUT = s"${DevSchema}.lk_lkc"
  val dashboardPath = s"${config.auxPath}lk_lkc"


  override val dashboardName: String = Nodet_team_k7m_aux_d_lk_lkcOUT //витрина
  override def processName: String = "lk_lkc"

  def DoLKLKC() {

    Logger.getLogger(Nodet_team_k7m_aux_d_lk_lkcOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(s"""
   select
     cast(row_number() over (order by x.link_id ) as string) lkc_id,
     x.link_id,
     CRIT_ID,
     FLAG_NEW             ,
     LINK_PROB,
     QUANTITY             ,
     U7M_ID_FROM,
     T_FROM,
     post_nm_from,
     U7M_ID_TO,
     T_TO
  from (
    select
         c.link_id,
         c.CRIT_ID              ,
         c.FLAG_NEW             ,
         c.LINK_PROB            ,
         c.QUANTITY             ,
         case
         when
          T_FROM = 'CLF' then f7m_id_from
          else
          cast(coalesce(c.u7m_id_from_by_crm,c.u7m_id_from_by_inn) as string)
         end U7M_ID_FROM          ,
         c.T_FROM               ,
          c.post_nm_from,
         cast(coalesce(c.u7m_id_to_by_crm,c.u7m_id_to_by_inn) as string)  as U7M_ID_TO,
         c.T_TO
    from $Node1t_team_k7m_aux_d_lk_lkcIN c
     ) x
 where U7M_ID_FROM is not null
   and U7M_ID_TO is not null""").write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_lk_lkcOUT")

    logInserted()
    logEnd()
  }
}
