package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class Rep1600FinalClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1Report1600FinalClassIN = s"${DevSchema}.k7m_integrum_1600"
  val Node2Report1600FinalClassIN = s"${DevSchema}.k7m_fok_reports"
  val NodeReport1600FinalClassOUT = s"${DevSchema}.k7m_rep1600_final"
  val dashboardPath = s"${config.auxPath}k7m_rep1600_final"


  override val dashboardName: String = NodeReport1600FinalClassOUT //витрина
  override def processName: String = "EL"

  def DoRep1600() {

    Logger.getLogger(NodeReport1600FinalClassOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  ul_inn,
  ta1600,
  cast(dt as timestamp) dt,
  source
  from (
    select
      ul_inn,
    ta1600,
    dt,
    source,
    row_number() over (partition by ul_inn order by dt desc) rn
      from (
        select
          ul_inn ,
        cast(dt as date) dt,
        cast(fs_value as double) ta1600,
        'I' source
          from $Node1Report1600FinalClassIN
      where fs_value<>00
      union all
      select
      inn ul_inn,
    cast(dt as date) dt,
  case
  when measure = 'тыс.' then 1000
  when measure = 'млн.' then 1000000
  end*cast(ta1600 as double) ta1600,
  'F' source
    from $Node2Report1600FinalClassIN
  where measure in ('тыс.','млн.')
  and ta1600 <> 0
  ) x
  ) x
  where rn = 1
  """).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$NodeReport1600FinalClassOUT")

    logInserted()
    logEnd()
  }
}
