package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CritClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa
  val Node1t_team_k7m_aux_d_K7M_CritIN = s"${Stg0Schema}.CRM_CX_GSZ_CRIT_ADM"
  val Nodet_team_k7m_aux_d_K7M_CritOUT = s"${DevSchema}.K7M_Crit"
  val dashboardPath = s"${config.auxPath}K7M_Crit"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CritOUT //витрина
  override def processName: String = "CRM_BASE"

  def DoCRIT() {

    Logger.getLogger(Nodet_team_k7m_aux_d_K7M_CritOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s""" select
       row_id as ID,
       name,
       descr_text
  from $Node1t_team_k7m_aux_d_K7M_CritIN"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CritOUT")

    logInserted()
    logEnd()
  }


}
