package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class FokDocsHeaderLoanSbrfClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_pa_d_fok_docs_header_final_loanIN = s"${Stg0Schema}.fok_docs_header"
  val Node2t_team_k7m_pa_d_fok_docs_header_final_loanIN = s"${Stg0Schema}.fok_docs_data"
  val Nodet_team_k7m_pa_d_fok_docs_header_final_loanOUT = s"${DevSchema}.fok_docs_header_final_loan_sbrf"
  val dashboardPath = s"${config.auxPath}fok_docs_header_final_loan_sbrf"


  override val dashboardName: String = Nodet_team_k7m_pa_d_fok_docs_header_final_loanOUT //витрина
  override def processName: String = "FOK_BASE"

  def DoFokDocsHeaderLoanSbrf ()
  {
    Logger.getLogger(Nodet_team_k7m_pa_d_fok_docs_header_final_loanOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""
         select
                 dh.id,
                 dh.divid,
                 dh.period as period,
                 dh.year as year,
                 max(dd.ch) as fin_stmt_meas_cd
           from
                 $Node1t_team_k7m_pa_d_fok_docs_header_final_loanIN dh
                 join (
                     select
                             id,
                             -- сортировка для каждого id клиента и периода 1) сначала по статусу: ищем среди подтвержденных, если их нет - среди остальных
                             -- 2) затем по дате изменения: отбираем среди найденных самый новый заголовок
                             row_number() over (partition by formid, divid, year, period order by case when docs_status = 9 then 1 else 0 end desc, modify_date desc) rn
                       from
                             $Node1t_team_k7m_pa_d_fok_docs_header_final_loanIN
                      where
                             formid in ('SB0150')
                 ) mv on (dh.id = mv.id and mv.rn = 1) -- соединение по pk, чтобы избежать размножения
                 join $Node2t_team_k7m_pa_d_fok_docs_header_final_loanIN dd on (dh.id = dd.docs_header_id and dd.field_name = 'MEASURE' and dd.ch in ('млн.', 'тыс.'))
          where
                 dh.formid in ('SB0150')
          group by
                 dh.id,
                 dh.divid,
                 dh.period,
                 dh.year
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$dashboardPath")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_fok_docs_header_final_loanOUT")

    logInserted()
    logEnd()
  }
}

