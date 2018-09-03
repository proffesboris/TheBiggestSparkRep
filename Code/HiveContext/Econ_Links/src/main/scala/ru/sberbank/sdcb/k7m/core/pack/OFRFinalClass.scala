package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class OFRFinalClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_OFR_finalIN = s"${DevSchema}.k7m_integrum_OFR"
  val Node2t_team_k7m_aux_d_k7m_OFR_finalIN = s"${DevSchema}.k7m_fok_OFR_prepared"
  val Nodet_team_k7m_aux_d_k7m_OFR_finalOUT = s"${DevSchema}.k7m_OFR_final"
  val dashboardPath = s"${config.auxPath}k7m_OFR_final"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_OFR_finalOUT //витрина
  override def processName: String = "EL"

  def DoOFRFinal(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_OFR_finalOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  ul_inn,
  vyruchka revenue,
  total_expenses,
  cast(from_dt as timestamp) from_dt,
  cast(to_dt as timestamp) to_dt,
  source
  from (
    select
      ul_inn,
    vyruchka,
    total_expenses,
    from_dt,
    to_dt,
    source,
    row_number() over (partition by ul_inn order by rep_dt desc) rn
      from (
        select
          ul_inn,
        cast(concat(yr,'-12-25') as date) rep_dt,
  cast(concat(yr,'-01-01') as date) from_dt,
  cast(concat(yr,'-12-31') as date) to_dt,
  vyruchka,
  total_expenses          ,
  'I' source
    from    (
      select
        ul_inn,
      yr,
      vyruchka,
      total_expenses,
      row_number() over ( partition by ul_inn order by yr desc) rni
        from $Node1t_team_k7m_aux_d_k7m_OFR_finalIN
  where concat(yr,'-12-31')<'${dateString}'
  and concat(cast(cast(yr as int)+2 as string),'-12-31')>'${dateString}'
  --Берем только актуальные данные
  ) xi
  where rni = 1
  union all
    select
  inn ul_inn,
  rep_dt,
  add_months(rep_dt,-4*3+1) from_dt,
  last_day(rep_dt) to_dt,
  vyruchka,
  total_expenses          ,
  'F' source
    from $Node2t_team_k7m_aux_d_k7m_OFR_finalIN
  where last_day(rep_dt) > add_months(date'${dateString}',-4*3)
  --Берем только актуальные данные
  ) x
  ) y
  where rn = 1""").write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_OFR_finalOUT")

    logInserted()
    logEnd()
  }
}
