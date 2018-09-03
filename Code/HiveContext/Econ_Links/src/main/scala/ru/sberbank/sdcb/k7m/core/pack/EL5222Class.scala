package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5222Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL_5222IN = s"${DevSchema}.k7m_EL_5222_prep"
  val Nodet_team_k7m_aux_d_k7m_EL_5222OUT = s"${DevSchema}.k7m_EL_5222"
  val dashboardPath = s"${config.auxPath}k7m_EL_5222"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL_5222OUT //витрина
  override def processName: String = "EL"

  def DoEL5222(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL_5222OUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  cast(date'${dateString}' as timestamp) report_dt,
  '' from_cid,
  from_inn,
  '' to_cid,
  to_inn,
  concat('5.2.2.2',
  case
  when   quantity>=${SparkMainClass.qnt5222}
  then ''
  else
  '_g'
  end
  ) crit_code,
  case
      when   quantity>=${SparkMainClass.qnt5222}
      then 50
  else
      10
  end probability,
  quantity,
  typ_contr source,
  base_amt ,
  abs_amt absolute_value_amt
  from (
    select
      typ_contr,
    from_inn,
    to_inn,
    base_amt,
    abs_amt,
    abs_amt/base_amt quantity,
    row_number() over (partition by  from_inn,    to_inn  order by abs_amt/base_amt )     rn
      from
      $Node1t_team_k7m_aux_d_k7m_EL_5222IN
  where   base_amt>0
  ) x
  where rn = 1""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5222OUT")

    logInserted()
    logEnd()
  }
}
