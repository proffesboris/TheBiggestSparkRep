package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FokFinStmtDetailClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_pa_d_fok_fin_stmt_detailIN = s"${Stg0Schema}.fok_docs_data"
   val Nodet_team_k7m_pa_d_fok_fin_stmt_detailOUT = s"${DevSchema}.fok_fin_stmt_detail"
  val dashboardPath = s"${config.auxPath}fok_fin_stmt_detail"


  override val dashboardName: String = Nodet_team_k7m_pa_d_fok_fin_stmt_detailOUT //витрина
  override def processName: String = "FOK_BASE"

  def DoFokFinStmtDetail ()
  {
    Logger.getLogger(Nodet_team_k7m_pa_d_fok_fin_stmt_detailOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""
           select
                 docs_header_id,
                 sum(case when prd = 'Финансовый лизинг' then amount else 0 end) as leas_amt,
                 sum(case when prd = 'Займ' then amount else 0 end) as loan_amt,
                 sum(case when prd = 'Кредит' then amount else 0 end) as crd_amt
           from
                 (
                     select
                             docs_header_id,
                             substr(field_name, 1, 7),
                             sum(coalesce(nm, 0)) as amount,
                             max(ch) as prd
                       from
                             $Node1t_team_k7m_pa_d_fok_fin_stmt_detailIN
                      where
                             field_name in ('DN_4.01.3', 'DN_4.01.5', 'DN_1.01.3', 'DN_1.01.5')
                      group by docs_header_id,
                              substr(field_name, 1, 7)
                 ) a
          group by
                 docs_header_id
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$dashboardPath")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_fok_fin_stmt_detailOUT")

    logInserted()
    logEnd()
  }
}

