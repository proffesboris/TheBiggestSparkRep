package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class LKCClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Nodet_team_k7m_pa_d_lkcOUT = s"${config.aux}.lkc"
  val dashboardPath = s"${config.auxPath}lkc"


  override val dashboardName: String = Nodet_team_k7m_pa_d_lkcOUT //витрина
  override def processName: String = "lk_lkc"

  def DoLKC() {

    Logger.getLogger(Nodet_team_k7m_pa_d_lkcOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(s"""
                                             |select
                                             |        x.lkc_id,
                                             |        x.lk_id,
                                             |        x.crit_id,
                                             |        '$execId' exec_id, --Получить из системы логирования
                                             |        x.LINK_PROB            ,
                                             |        x.QUANTITY,
                                             |        x.post_nm_from
                                             |     FROM (
                                                   select
                                                     lk.lkc_id,
                                                     l.lk_id,
                                                     lk.crit_id,
                                                     lk.LINK_PROB            ,
                                                     lk.QUANTITY,
                                                     lk.post_nm_from,
                                                     --TODO перенести в прототип. Срочная правка SDCB-512. Именно по связям по EIOGEN_EKS уберем дубли позднее на этапе EKS_ID
                                                     row_number() over (
                                                       partition by
                                                             l.lk_id,
                                                             lk.crit_id,
                                                             case when
                                                                 lk.crit_id ='EIOGEN_EKS' then
                                                                 lk.link_id
                                                                 else ''
                                                                 end
                                                         order by lk.LINK_PROB desc,lk.QUANTITY desc,   lk.lkc_id
                                                       ) rn
                                             |     from
                                             |        ${config.aux}.lk_lkc lk
                                             |        left join  ${config.aux}.lk l
                                             |        on concat(l.T_FROM,cast(l.U7M_ID_FROM as string),l.T_TO ,cast(l.U7M_ID_TO as string) ) = concat(lk.T_FROM,cast(lk.U7M_ID_FROM as string),lk.T_TO ,cast(lk.U7M_ID_TO as string) )
                                             |     ) x
                                             |     where x.rn=1""".stripMargin).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_pa_d_lkcOUT")

    logInserted()

    //---------------------------------------------------------------------
    //--  Выполнить проверки:
    //  -- Записать расхождения в журнал CUSTOM_LOG с типом ERR
    //---------------------------------------------------------------------

    //Нарушена целостность связи LK-LKC
    val checkStage1 = spark.sql(s"""
    select crit_id, cast(count(*) as string) cnt
from ${config.aux}.lkc
where lk_id is null
group by         crit_id""").collect().toSeq

    val log1 = checkStage1.foreach(c=>
      log("LKC","Нарушена целостность связи LK-LKC. "+c.getString(0)+": "+c.getString(1), CustomLogStatus.ERROR))

    //FROM равен TO
    val checkStage2 = spark.sql(s"""
    select  cast(count(*) as string) cnt
from ${config.aux}.lk
where U7M_ID_FROM = U7M_ID_TO""").collect().toSeq

    val log2 = checkStage2.foreach(c=>
      log("LKC","FROM равен TO: "+c.getString(0), CustomLogStatus.ERROR))

    logEnd()
  }
}
