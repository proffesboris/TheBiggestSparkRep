package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FokFinStmtLoanSbrfClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_pa_d_fok_fin_stmt_detailIN = s"${Stg0Schema}.fok_docs_data"
  val Node2t_team_k7m_pa_d_fok_fin_stmt_detailIN = s"${DevSchema}.fok_docs_header_final_loan_sbrf"
  val Nodet_team_k7m_pa_d_fok_fin_stmt_detailOUT = s"${DevSchema}.fok_fin_stmt_debt_sbrf"
  val dashboardPath = s"${config.auxPath}fok_fin_stmt_detail_sbrf"


  override val dashboardName: String = Nodet_team_k7m_pa_d_fok_fin_stmt_detailOUT //витрина
  override def processName: String = "FOK_BASE"

  def DoFokFinStmtLoanSbrf ()
  {
    Logger.getLogger(Nodet_team_k7m_pa_d_fok_fin_stmt_detailOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""
         select
                crm_cust_id,
                fin_stmt_year,
                fin_stmt_period,
                cast(fin_stmt_start_dt as timestamp) as fin_stmt_start_dt,
                cast(fin_stmt_end_dt as timestamp) as fin_stmt_end_dt,
                cast(fin_stmt_rep_dt as timestamp) as fin_stmt_rep_dt,
                case when fin_stmt_meas_cd = 'млн.' then fin_loan_debt_amt * 1000 else fin_loan_debt_amt end as fin_loan_debt_amt
         from
                (
                    select
                            dh.divid as crm_cust_id,
                            dh.year as fin_stmt_year,
                            dh.period as fin_stmt_period,
                            dh.fin_stmt_meas_cd,
                            case
                                when dh.period in (1,2,3,4) and year > 1900 then case
                                      when dh.period = 1 then to_date(concat(year,'-01-01'))
                                      when dh.period = 2 then to_date(concat(year,'-04-01'))
                                      when dh.period = 3 then to_date(concat(year,'-07-01'))
                                      when dh.period = 4 then to_date(concat(year,'-10-01'))
                                  end
                                else null
                            end as fin_stmt_start_dt,
                           case
                               when dh.period in (1,2,3,4) and year > 1900 then case
                                 when dh.period = 1 then to_date(concat(year,'-03-31'))
                                 when dh.period = 2 then to_date(concat(year,'-06-30'))
                                 when dh.period = 3 then to_date(concat(year,'-09-30'))
                                 when dh.period = 4 then to_date(concat(year,'-12-31'))
                                 end
                               else null
                            end as fin_stmt_end_dt,
                            case
                               when dh.period in (1,2,3,4) and year > 1900 then case
                                 when dh.period = 1 then to_date(concat(year,'-04-01'))
                                 when dh.period = 2 then to_date(concat(year,'-07-01'))
                                 when dh.period = 3 then to_date(concat(year,'-10-01'))
                                 when dh.period = 4 then to_date(concat(year + 1,'-01-01'))
                                 end
                                else null
                            end as fin_stmt_rep_dt,
                            sum(coalesce(cast(dd.nm as decimal(18,6)), 0)) as fin_loan_debt_amt
                      from
                            $Node2t_team_k7m_pa_d_fok_fin_stmt_detailIN dh
                            join $Node1t_team_k7m_pa_d_fok_fin_stmt_detailIN dd on (dh.id = dd.docs_header_id and dd.field_name like 'DNS_1.__.6')
                      group by
                             dh.divid,
                             dh.year,
                             dh.period,
                             dh.fin_stmt_meas_cd,
                             fin_stmt_start_dt, fin_stmt_end_dt, fin_stmt_rep_dt
                ) sel
         where
                fin_stmt_start_dt < current_timestamp and
                fin_stmt_start_dt is not NULL
         
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

