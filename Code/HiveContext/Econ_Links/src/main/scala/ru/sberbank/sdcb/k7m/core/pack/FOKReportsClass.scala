package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FOKReportsClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_fok_reportsIN = s"${Stg0Schema}.fok_docs_data"
  val Node2t_team_k7m_aux_d_k7m_fok_reportsIN = s"${DevSchema}.k7m_fok_header_selected"
  val Nodet_team_k7m_aux_d_k7m_fok_reportsOUT = s"${DevSchema}.k7m_fok_reports"
  val dashboardPath = s"${config.auxPath}k7m_fok_reports"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_fok_reportsOUT //витрина
  override def processName: String = "EL"

  def DoFOKReports() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_fok_reportsOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
    dh.SBRF_INN as inn,
    concat(dh.year,'-',lpad(cast(cast(dh.period as smallint)*3 as string),2,'0'),'-28') dt ,
    max(case when dd.field_name = "MEASURE"  then dd.ch end)  as measure, -- тыс., млн.
    cast(cast(max(case when dd.field_name = "BB1.1600.3"  then dd.nm end) as double) as decimal(17,2) ) as TA1600
  from $Node1t_team_k7m_aux_d_k7m_fok_reportsIN dd
  join $Node2t_team_k7m_aux_d_k7m_fok_reportsIN dh
    on dd.docs_header_id = dh.id
 group by dh.SBRF_INN, dh.year, dh.period
      """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_fok_reportsOUT")

    logInserted()
    logEnd()
  }


}
