package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql._

//2. выручка LTM по транзакциям
class Target_revenue_L12M(override val spark: SparkSession, val config: Config, date: LocalDate) extends EtlLogger with EtlJob {

  val stgSchema = config.stg
  val targetAuxSchema = config.aux
  val targetPaSchema = config.pa

  val dashboardName = s"$targetAuxSchema.Target_revenue_L12M"

  val dashboardPath = s"${config.auxPath}Target_revenue_L12M"
  override def processName: String = "SBL"

  def run(): Unit = {
    logStart()


    val (date_cond_from, date_cond_to) = (date.minusYears(1), date)

    val sql_query = s"""
        select  c_kl_kt_2_inn as inn,
                '%s' as dt,
                sum(cast(c_sum_nt as decimal(23,5))) as revenue_L12M
        from $targetAuxSchema.trbasis_kras
        where PREDICTED_VALUE in (
                                    'эквайринг',
                                    'инкассация',
                                    'оплата по договору',
                                    'лизинг',
                                    'аренда',
                                    'дивиденды'
                                    )
                and
                (
                    c_kl_kt_2_inn  in
                        (
                            select org_inn_crm_num from $targetAuxSchema.basis_client
                        )
                )
                and (c_date_prov >= '%s')
                and (c_date_prov < '%s')
        group by c_kl_kt_2_inn
    """.format(date_cond_to, date_cond_from, date_cond_to)

    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(dashboardName)

    logInserted()
    logEnd()
  }

}