package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql._

class Dividends_OUT(override val spark: SparkSession, val config: Config, date: LocalDate) extends EtlLogger with EtlJob {

  val stgSchema = config.stg
  val targetAuxSchema = config.aux
  val targetPaSchema = config.pa

  override val dashboardName = s"$targetAuxSchema.crit_dividends_out"
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_dividends_out"


  def run(): Unit = {
    import SparkMain._

    logStart()


    val (date_cond_from, date_cond_to) = (date.minusYears(1), date)

    val div_df_query = s"""

                     select

                            case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn_dt
                            ,case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn_kt
                            , '%s' as dt
                            , sum(v.c_sum_nt) as sum_dividends_out

                    from
                    $targetAuxSchema.trbasis_kras v
                    where (v.predicted_value in ('дивиденды'))
                         and (v.c_date_prov >= '%s')
                         and (v.c_date_prov < '%s')
                         and ((case when v.ktdt = 0 then v.inn_sec else v.inn_st end) in (select org_inn_crm_num from $targetAuxSchema.basis_client))
                         and ((case when v.ktdt = 0 then v.inn_st else v.inn_sec end) <> '%s')
                         and (filt = 0)
                    group by (case when v.ktdt = 0 then v.inn_sec else v.inn_st end), (case when v.ktdt = 0 then v.inn_st else v.inn_sec end)
    """.format(date_cond_to, date_cond_from, date_cond_to, SBRF_INN)

    spark.sql(div_df_query).createOrReplaceTempView("div_df")

    val sql_query = s"""

                 select
                             sl.inn_dt as inn1
                             ,sl.inn_kt as inn2
                             ,sl.dt
                             ,sl.sum_dividends_out
                             ,case when re.revenue_L12M_true < 1 then 0
                                   else (sl.sum_dividends_out / re.revenue_L12M_true)
                               end as quantity
                             ,re.revenue_L12M_true
                 from

                        div_df sl
                        left join
                        $targetAuxSchema.Revenue_Target re
                        on (sl.inn_dt = re.inn) and (sl.dt = re.dt)

                """

    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(dashboardName)

    logInserted()
    logEnd()
  }

}