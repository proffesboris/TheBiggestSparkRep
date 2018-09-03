package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// 4. Обогатим исходную выборку выручкой
// 5. Сохраним посчитанную выручку
class Revenue_Target(override val spark: SparkSession, val config: Config, date: LocalDate) extends EtlLogger with EtlJob {

  val stgSchema = config.stg
  val targetAuxSchema = config.aux
  val targetPaSchema = config.pa

  override val dashboardName = s"$targetAuxSchema.Revenue_Target"
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}Revenue_Target"


  def run(paramDate: String): Unit = {
    import SparkMain._

    logStart()


    val date_cond_to = date

    val inn_df = spark.sql(s"select org_inn_crm_num inn from $targetAuxSchema.basis_client").cache()
    inn_df.count()
    val revLY_df = spark.sql(s"select inn, revenue_LY from $targetAuxSchema.Target_revenue_LY")
    val revRO_df = spark.sql(s"select inn, revenue_rosstat from $targetAuxSchema.Target_Revenue_Rosstat")
    val revLTM_df = spark.sql(s"select inn, revenue_L12M from $targetAuxSchema.Target_revenue_L12M")
    val jnd_revLTM_df = revLTM_df.join(inn_df, Seq("inn"), "right")
    val jnd_revLTM_RO_df = jnd_revLTM_df.join(revRO_df, Seq("inn"), "left")
    val jnd_revLTM_RO_LY_df = jnd_revLTM_RO_df.join(revLY_df, Seq("inn"), "left")

    val sql_query = s"""
 select
        inn
        , dt
        , revenue_L12M
        , revenue_rosstat
        , revenue_LY
        , case when revenue_rosstat < 1 and  revenue_L12M > %s then revenue_L12M -- больше revenue_down_limit
               when revenue_rosstat < 1 and  revenue_L12M <= %s  then 0 -- меньше revenue_down_limit
               when revenue_rosstat > 1 and  (alpha < %s ) and (alpha > %s ) then (cast(alpha as decimal(28,15)) * revenue_rosstat) -- между 0.8 и 1.2
               when revenue_rosstat > 1 and  (alpha >= %s ) and (alpha <= %s)  then (%s  * revenue_rosstat) --CONST_REVENUE_UP_LIMIT
               when revenue_rosstat > 1 and  (alpha <= %s ) and (alpha >= %s)  then (%s  * revenue_rosstat) --CONST_REVENUE_DOWN_LIMIT
               when revenue_rosstat > 1 and   alpha < %s then revenue_rosstat
               when revenue_rosstat > 1 and   alpha > %s then revenue_rosstat
               else -99999999
          end as revenue_L12M_true
from (select
            inn
            ,'%s' dt
            , coalesce(revenue_L12M, 0) as revenue_L12M
            , coalesce(revenue_rosstat, 0) as revenue_rosstat
            , coalesce(revenue_LY, 0) as  revenue_LY
            , case when coalesce(revenue_LY, 0) > 1 then coalesce(coalesce(revenue_L12M, 0) / coalesce(revenue_LY, 0) , 0)
                else -1
              end as alpha
       from jnd_revLTM_RO_LY_df)
      """.format(revenueDownLimit,
      revenueDownLimit,
      CONST_REVENUE_UP_LIMIT, CONST_REVENUE_DOWN_LIMIT,
      CONST_REVENUE_UP_LIMIT, CONST_REVENUE_CUT_OFF_HIGH, CONST_REVENUE_UP_LIMIT,
      CONST_REVENUE_DOWN_LIMIT, CONST_REVENUE_CUT_OFF_LOW, CONST_REVENUE_DOWN_LIMIT,
      CONST_REVENUE_CUT_OFF_LOW,
      CONST_REVENUE_CUT_OFF_HIGH,
      paramDate)

    jnd_revLTM_RO_LY_df.createOrReplaceTempView("jnd_revLTM_RO_LY_df")

    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(dashboardName)

    logInserted()
    logEnd()
  }

}