package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class LKClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux



  val Node1t_team_k7m_pa_d_lkIN = s"${DevSchema}.lk_lkc"
  val Nodet_team_k7m_pa_d_lkOUT = s"${config.aux}.lk"
  val dashboardPath = s"${config.auxPath}lk"


  override val dashboardName: String = Nodet_team_k7m_pa_d_lkOUT //витрина
  override def processName: String = "lk_lkc"

  def DoLK() {

    Logger.getLogger(Nodet_team_k7m_pa_d_lkOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(s"""
  select
            cast( row_number() over (order by U7M_ID_FROM, U7M_ID_TO) as string) lk_id,
            T_FROM,
            U7M_ID_FROM,
            T_TO,
            U7M_ID_TO,
            FLAG_NEW,
            '$execId' exec_id --Получить из системы логирования
    from
     (
        select
            T_FROM,
            U7M_ID_FROM,
            T_TO,
            U7M_ID_TO,
            max(FLAG_NEW) FLAG_NEW
        from $Node1t_team_k7m_pa_d_lkIN
        group by
            T_FROM,
            U7M_ID_FROM,
            T_TO,
            U7M_ID_TO
        )     x""").write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_pa_d_lkOUT")

    logInserted()
    logEnd()
  }
}
