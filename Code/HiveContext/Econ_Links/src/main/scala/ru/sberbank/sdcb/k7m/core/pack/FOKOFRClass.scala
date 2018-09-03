package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FOKOFRClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_fok_OFRIN = s"${Stg0Schema}.fok_docs_data"
  val Node2t_team_k7m_aux_d_k7m_fok_OFRIN = s"${DevSchema}.k7m_fok_header_selected"
  val Nodet_team_k7m_aux_d_k7m_fok_OFROUT = s"${DevSchema}.k7m_fok_OFR"
  val dashboardPath = s"${config.auxPath}k7m_fok_OFR"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_fok_OFROUT //витрина
  override def processName: String = "EL"

  def DoFOKOFR() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_fok_OFROUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
    dh.SBRF_INN as inn,
    dh.year as yr,
    dh.period as qt,
    max(case when dd.field_name = 'MEASURE'  then dd.ch end)  as measure, -- тыс., млн.
    cast(sum(case when dd.field_name = 'PU1.2110.3'  then cast(dd.nm  as double)end) as decimal(17,2) ) as vyruchka,
    cast(sum(case when dd.field_name in ('PU1.2210.3','PU1.2350.3','PU1.2220.3','PU1.2120.3')  then cast(dd.nm  as double) end)  as decimal(17,2) ) as total_expenses
  from $Node1t_team_k7m_aux_d_k7m_fok_OFRIN dd
  join $Node2t_team_k7m_aux_d_k7m_fok_OFRIN dh
    on dd.docs_header_id = dh.id
 where dd.field_name in ( ${SparkMainClass.fieldName} )
 group by dh.SBRF_INN, dh.year, dh.period
      """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_fok_OFROUT")

    logInserted()
    logEnd()
  }
}
