package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class IntegrumOFRClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_integrum_OFRIN = s"${DevSchema}.k7m_integrum_org"
  val Node2t_team_k7m_aux_d_k7m_integrum_OFRIN = s"${Stg0Schema}.int_financial_statement_rosstat"
  val Nodet_team_k7m_aux_d_k7m_integrum_OFROUT = s"${DevSchema}.k7m_integrum_OFR"
  val dashboardPath = s"${config.auxPath}k7m_integrum_OFR"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_integrum_OFROUT //витрина
  override def processName: String = "EL"

  def DoIntOFR() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_integrum_OFROUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      regexp_replace(f.period_nm,' год','') yr,
      f.ul_org_id,
      f.fs_unit,
      o.ul_inn,
      sum(case when fs_line_num in (${SparkMainClass.fsLineNumProfit}) then cast(f.fs_value as double) else 0 end) vyruchka,
      sum(case when fs_line_num in (${SparkMainClass.fsLineNumTotal}) then cast(f.fs_value as double) else 0 end) total_expenses
  from $Node1t_team_k7m_aux_d_k7m_integrum_OFRIN  o
  join $Node2t_team_k7m_aux_d_k7m_integrum_OFRIN  f
    on f.ul_org_id = o.ul_org_id
 where fs_form_num=2 and fs_column_nm='За отч. период' and fs_line_num in(${SparkMainClass.fsLineNumProfit},${SparkMainClass.fsLineNumTotal})
 group by regexp_replace(f.period_nm,' год',''),
      f.ul_org_id,
      f.fs_unit,
      o.ul_inn
      """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_integrum_OFROUT")

    logInserted()
    logEnd()
  }


}
