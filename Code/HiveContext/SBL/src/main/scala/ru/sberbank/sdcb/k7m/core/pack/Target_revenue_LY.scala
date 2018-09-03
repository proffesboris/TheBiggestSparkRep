package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate
import java.time.temporal.TemporalAdjusters.firstDayOfYear
import java.time.temporal.TemporalAdjusters.lastDayOfYear

import org.apache.spark.sql._

// 3. Выручка за последний календарный год по транзакциям
class Target_revenue_LY(override val spark: SparkSession, val config: Config, date: LocalDate) extends EtlLogger with EtlJob {

  val stgSchema = config.stg
  val targetAuxSchema = config.aux
  val targetPaSchema = config.pa
  val dashboardPath = s"${config.auxPath}Target_revenue_LY"
  override def processName: String = "SBL"
  val dashboardName = s"$targetAuxSchema.Target_revenue_LY"


  def run(): Unit = {
    import SparkMain._

    logStart()


    val (date_cond_from, date_cond_to) = (date.minusYears(1), date)
    val date_cond_LY_from = date_cond_from `with` firstDayOfYear
    val date_cond_LY_to = date_cond_from `with` lastDayOfYear

    val sql_query = s"""
select a.inn
        , a.dt
        , case when (b.revenue_rosstat < 1) or (b.revenue_rosstat is null) then a.revenue_LY
               when  a.revenue_LY/b.revenue_rosstat > %s then 0
               when  a.revenue_LY/b.revenue_rosstat < %s then 0
               else a.revenue_LY
               end as revenue_LY

     from (
       select  c_kl_kt_2_inn as inn,
              '%s' as dt,
              sum(cast(c_sum_nt as decimal(23,5))) as revenue_LY

      from $targetAuxSchema.trbasis_kras v
      where v.predicted_value in (
                                  'эквайринг',
                                  'инкассация',
                                  'оплата по договору',
                                  'лизинг',
                                  'аренда',
                                  'дивиденды'
                                  )
              and
                  c_kl_kt_2_inn in
                      (select org_inn_crm_num from $targetAuxSchema.basis_client
                      )
              and (v.c_date_prov <= '%s')
              and (v.c_date_prov >= '%s')
      group by c_kl_kt_2_inn) a
      left join  $targetAuxSchema.Target_Revenue_Rosstat b
             on a.inn = b.inn
    """.format(CONST_REVENUE_CUT_OFF_HIGH, CONST_REVENUE_CUT_OFF_LOW, date_cond_to, date_cond_LY_to, date_cond_LY_from)

    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(dashboardName)

    logInserted()
    logEnd()
  }

}