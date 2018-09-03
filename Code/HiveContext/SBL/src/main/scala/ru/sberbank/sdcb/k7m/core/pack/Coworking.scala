package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql._

// 12. Простые товарищества и совместная деятельность
class Coworking(override val spark: SparkSession, val config: Config, date: LocalDate) extends EtlLogger with EtlJob {

  val stgSchema = config.stg
  val targetAuxSchema = config.aux
  val targetPaSchema = config.pa

  override val dashboardName = s"$targetAuxSchema.crit_coworking"
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_coworking"


  def run(): Unit = {
    import SparkMain._

    logStart()


    val date_cond = date

    val all_coworking_query = s"""
      select
                  v.id
                  ,case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn_dt
                  ,case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn_kt
                  ,c_date_prov
      from $targetAuxSchema.trbasis_kras v
            where (((c_nazn like '%совместной%') and (c_nazn like '%деятельности%')) or
      ( (c_nazn like '%ростого%') and (c_nazn like '%товарищества%')))

      and (filt = 0)

      """

    spark.sql(all_coworking_query).createOrReplaceTempView("all_coworking")

    val sql_query = s"""

            select
                 inn_dt as inn1
                 ,inn_kt as inn2
                 , '%s' as dt
                 , count(id) as quantity

            from all_coworking
            where    c_date_prov < '%s'
                    and inn_dt  <> '%s'
                    and inn_kt <> '%s'
                    and (
                    (inn_kt in (select org_inn_crm_num from $targetAuxSchema.basis_client)) or
                   (inn_dt in (select org_inn_crm_num from $targetAuxSchema.basis_client))
                   )
            group by inn_dt, inn_kt

    """.format(date_cond, date_cond, SBRF_INN, SBRF_INN)

    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(dashboardName)

    logInserted()
    logEnd()
  }

}