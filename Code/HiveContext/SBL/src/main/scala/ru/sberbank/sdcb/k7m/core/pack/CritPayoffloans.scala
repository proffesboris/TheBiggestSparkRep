package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql.SparkSession

/**
  * 7. Гашение займов в пользу ЮЛ
  */
class CritPayoffloans(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val auxSchema = config.aux
  val SBRF_INN = "7707083893"



  override val dashboardName: String = s"${config.aux}.crit_payoffloans"
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_payoffloans"

  def run(date: LocalDate): Unit = {
    execute(date.minusYears(1).toString, date.toString)
  }

  def execute(date_cond_from: String, date_cond_to: String): Unit = {
    logStart()
    var payoffloans_df = spark.sql(s"select '' as inn_dt, '' as inn_kt, '' as dt, '' as sum_out_loans from $auxSchema.basis_client where (1 <> 1)")
    var sql_query = s"""
                     select
           case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn_dt
           ,case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn_kt
           , '$date_cond_to' as dt
           , sum(v.c_sum_nt) as sum_out_loans


           from $auxSchema.trbasis_kras v
                where (v.predicted_value in ('гашение займа')) and
           ( (case when v.ktdt = 0 then v.inn_st else v.inn_sec end) in (select org_inn_crm_num from $auxSchema.basis_client) )
                    and (v.c_date_prov >= '$date_cond_from')
                    and (v.c_date_prov < '$date_cond_to')
                    and v.inn_st  <> '$SBRF_INN'
                    and v.inn_sec <> '$SBRF_INN'
                    and (filt = 0)

          group by (case when v.ktdt = 0 then v.inn_sec else v.inn_st end), (case when v.ktdt = 0 then v.inn_st else v.inn_sec end)
    """
    payoffloans_df = payoffloans_df.union(spark.sql(sql_query))
    payoffloans_df.createOrReplaceTempView("payoffloans")

    sql_query = s"""
                 select
                     distinct
                             sl.inn_dt as inn1
                             ,sl.inn_kt as inn2
                             ,sl.dt
                             ,sl.sum_out_loans
                            ,(sl.sum_out_loans / re.revenue_L12M_true) as quantity
                            ,re.revenue_L12M_true
                 from

                        payoffloans sl
                        left join
                        $auxSchema.Revenue_Target re
                        on sl.inn_kt = re.inn --and sl.dt = re.dt"""
    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath.concat("_temp"))
      .saveAsTable(dashboardName.concat("_temp"))

    import SparkMain._
    sql_query = s"""
    select d.*
    from
    (
      select rank() over (partition by inn2, dt order by quantity desc) as rn
      , inn2
      , dt
      , inn1
      , quantity
      from ${dashboardName.concat("_temp")} a
    ) d
      where d.rn <= $CONST_TOP_PAYOFFLOANS_COUNT
    """

    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath.concat("_l1"))
      .saveAsTable(dashboardName.concat("_l1"))

   sql_query = s"""
    select d.*
    from
    (
      select rank() over (partition by inn1, dt order by quantity desc) as rn
      , inn2
      , dt
      , inn1
      , quantity
      from ${dashboardName.concat("_l1")} a
    ) d
      where d.rn <= $CONST_TOP_PAYOFFLOANS_COUNT
    """

    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath)
      .saveAsTable(dashboardName)

    logInserted()
    logEnd()
  }
}
