package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FOKOFRPreparedClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_fok_OFR_preparedIN = s"${DevSchema}.k7m_fok_OFR"
  val Nodet_team_k7m_aux_d_k7m_fok_OFR_preparedOUT = s"${DevSchema}.k7m_fok_OFR_prepared"
  val dashboardPath = s"${config.auxPath}k7m_fok_OFR_prepared"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_fok_OFR_preparedOUT //витрина
  override def processName: String = "EL"

  def DofokOFRPrep(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_fok_OFR_preparedOUT).setLevel(Level.WARN)

    logStart()



    val createHiveTableStage1 = spark.sql(
      s"""select
  inn,
  cast(max(rep_dt) as timestamp) rep_dt,
  max_period,
  sum(vyruchka) vyruchka,
  sum(total_expenses) total_expenses
    from (
      select
        inn,
      yr,
      qt,
      cast( concat(yr,'-',lpad(cast(cast(qt as smallint)*3 as string),2,'0'),'-01') as date) rep_dt,
  period,
  vyruchka - lag(vyruchka,1,0) over (partition by inn,yr order by qt) vyruchka,
  total_expenses -lag(total_expenses,1,0) over (partition by inn,yr order by qt) total_expenses,
  lag(vyruchka,1,0) over (partition by inn,yr order by qt) prev_vyruchka,
  lag(total_expenses,1,0) over (partition by inn,yr order by qt) prev_total_expenses,
  max(period) over  (partition by inn) max_period
    from (
      select
        inn,
      yr,
      qt,
  case
  when measure = 'тыс.' then 1000
  when measure = 'млн.' then 1000000
  end*cast(vyruchka as double) as vyruchka,
  case
  when measure = 'тыс.' then 1000
  when measure = 'млн.' then 1000000
  end*cast(total_expenses as double) as total_expenses         ,
  cast(yr as int)*4+qt period
    from $Node1t_team_k7m_aux_d_k7m_fok_OFR_preparedIN
  where concat(yr,'-',lpad(cast(cast(qt as smallint)*3 as string),2,'0'),'-28') <'${dateString}' --YYYY-MM-DD
  --where inn ='0101006023'
  ) x
  ) y
  where   period between   max_period-3 and max_period
  and not (prev_vyruchka+prev_total_expenses=0 and qt>1 )
  group by
    inn,
  max_period
  having count(*) =4""") .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_fok_OFR_preparedOUT")

    logInserted()
    logEnd()
  }
}
