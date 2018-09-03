package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EL5213Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_EL_5213IN = s"${DevSchema}.k7m_EL_5213_prep"
  val Node2t_team_k7m_aux_d_k7m_EL_5213IN = s"${DevSchema}.basis_client"
  val Nodet_team_k7m_aux_d_k7m_EL_5213OUT = s"${DevSchema}.k7m_EL_5213"
  val dashboardPath = s"${config.auxPath}k7m_EL_5213"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_EL_5213OUT //витрина
  override def processName: String = "EL"

  def DoEL5213(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_EL_5213OUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""select
  cast (date'${dateString}' as timestamp) report_dt,
  '' from_cid,
  from_inn,
  '' to_cid,
  to_inn,
  concat('5.2.1.3',
  case
  when   quantity>=${SparkMainClass.qnt5213}
  then ''
  else
  '_g'
  end
  ) crit_code,
  100 probability,
  quantity,
  cred_id source,
  issue_amt base_amt,
  abs_amt absolute_value_amt
    from (
      select
        from_inn,
      to_inn,
      cred_id,
      issue_dt,
      issue_amt,
      abs_amt,
      quantity,
      row_number() over (partition by from_inn,to_inn order by quantity desc) rn

        from (
          select
            from_inn,
          to_inn,
          cred_id,
          issue_dt,
          sum(abs_amt) abs_amt,
          issue_amt,
          sum(abs_amt)/issue_amt quantity
            from $Node1t_team_k7m_aux_d_k7m_EL_5213IN
        where issue_amt>0
        group by
        from_inn,
      to_inn,
      cred_id,
      issue_dt,
      issue_amt
    ) x
  ) x
  join      (
    select distinct org_inn_crm_num org_inn from $Node2t_team_k7m_aux_d_k7m_EL_5213IN
      where org_inn_crm_num is not null) b
  on b.org_inn =  x.to_inn
  where rn=1     """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_EL_5213OUT")

    logInserted()
    logEnd()
  }
}
