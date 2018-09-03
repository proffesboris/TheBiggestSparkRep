package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class Integrum1600Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_integrum_1600IN = s"${Stg0Schema}.int_financial_statement_rosstat"
  val Node2t_team_k7m_aux_d_k7m_integrum_1600IN = s"${DevSchema}.k7m_integrum_org"
  val Nodet_team_k7m_aux_d_k7m_integrum_1600OUT = s"${DevSchema}.k7m_integrum_1600"
  val dashboardPath = s"${config.auxPath}k7m_integrum_1600"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_integrum_1600OUT //витрина
  override def processName: String = "EL"

  def DoInt1600() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_integrum_1600OUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
      distinct
      regexp_replace(f.period_nm,' год','-12-25') dt,
      f.ul_org_id,
      f.fs_unit,
      f.fs_value,
      o.ul_inn
  from $Node1t_team_k7m_aux_d_k7m_integrum_1600IN  f
  join $Node2t_team_k7m_aux_d_k7m_integrum_1600IN  o
    on f.ul_org_id = o.ul_org_id
 where fs_column_nm = ${SparkMainClass.fsColumnNm}
   and fs_form_num =1
   and fs_line_num =${SparkMainClass.fsLineNum1600}
      """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_integrum_1600OUT")

    logInserted()
    logEnd()
  }
}
