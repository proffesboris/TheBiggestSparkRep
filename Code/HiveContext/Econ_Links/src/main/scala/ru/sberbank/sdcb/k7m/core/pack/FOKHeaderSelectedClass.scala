package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FOKHeaderSelectedClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_fok_header_selectedIN = s"${DevSchema}.k7m_fok_header"
  val Nodet_team_k7m_aux_d_k7m_fok_header_selectedOUT = s"${DevSchema}.k7m_fok_header_selected"
  val dashboardPath = s"${config.auxPath}k7m_fok_header_selected"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_fok_header_selectedOUT //витрина
  override def processName: String = "EL"

  def DoFOKHeaderSelected() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_fok_header_selectedOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
    a.*
from
    $Node1t_team_k7m_aux_d_k7m_fok_header_selectedIN a
    inner join
    (select
    SBRF_INN,
    divid,
    formid,
    year,
    period,
    max(case when docs_status=9 then version end) as mv_9,
    max(version) as mv_all,
    coalesce(max(case when docs_status=9 then version end),max(version) ) as mv
from
     $Node1t_team_k7m_aux_d_k7m_fok_header_selectedIN
group by
    SBRF_INN, divid, formid, year, period) mv
    on a.SBRF_INN=mv.SBRF_INN and a.divid=mv.divid and a.year=mv.year and a.period=mv.period and a.version=mv.mv
where
    not (a.SBRF_INN=${SparkMainClass.excludeSBRFInn} and a.divid=${SparkMainClass.excludeDivid} and a.DOCS_STATUS=3 and a.year=2016 and a.period=3)
      """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_fok_header_selectedOUT")

    logInserted()
    logEnd()
  }
}
