package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql._

//1. Выручка Росстат
class Target_Revenue_Rosstat(override val spark: SparkSession, val config: Config, date: LocalDate) extends EtlLogger with EtlJob {

  val stgSchema = config.stg
  val targetAuxSchema = config.aux
  val targetPaSchema = config.pa

  override val dashboardName = s"$targetAuxSchema.Target_Revenue_Rosstat"
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}Target_Revenue_Rosstat"


  def run(revenueDownLimit: String): Unit = {

    logStart()


    val date_cond_to = date
    val year_cond = date_cond_to.getYear - 1 + " год"
    val year_cond_beflast = date_cond_to.getYear - 2 + " год"
    val sql_query = s"""select
            dd.inn
             , '%s' as dt
             , case when greatest(max(dd.rev_last), max(dd.rev_beflast)) < %s then 0
                    else greatest(max(dd.rev_last), max(dd.rev_beflast))
                    end as revenue_rosstat
    from (
        select
                c.inn
                , case when period_nm = '%s' and revenue_rosstat is null then 0
                       when period_nm = '%s' and revenue_rosstat is not null then revenue_rosstat
                    else 0 end as rev_beflast
                , case when period_nm = '%s' and revenue_rosstat is null then 0
                       when period_nm = '%s' and revenue_rosstat is not null then revenue_rosstat
                    else 0 end as rev_last
        from(
                select
                            b.ul_inn as inn
                            , period_nm
                            , max(cast(r.fs_value as decimal(22,5))) as revenue_rosstat
                from
                    ( select ul_org_id, fs_value, period_nm
                        from
                       $stgSchema.int_financial_statement_rosstat
                        where  (
                                   (fs_line_num = '2110')
                                    and (period_nm in ('%s', '%s'))
                                    and (fs_line_cd = 'P21103')
                                )
                    ) r

                left join
                (select a.ul_org_id, a.ul_inn
                   from (select row_number() over (partition by ul_org_id order by effectivefrom desc) as rn,*
                           from $stgSchema.int_ul_organization_rosstat
                          where --effectiveto >= '%s'
                           -- and
                            effectivefrom < '%s'
                ) a
                where a.rn=1
                )b
                on r.ul_org_id = b.ul_org_id
                left join
                            (
                                select org_inn_crm_num inn from $targetAuxSchema.basis_client
                            ) ef
                on b.ul_inn =  ef.inn
                where ef.inn is not null
                group by b.ul_inn, period_nm
        ) c
    ) dd
    group by dd.inn
  """.format(date_cond_to, revenueDownLimit,
      year_cond,
      year_cond_beflast,
      year_cond_beflast,
      year_cond,
      year_cond, year_cond_beflast,
      date_cond_to, date_cond_to)

  spark.sql(sql_query)
    .write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .option("path", dashboardPath).saveAsTable(dashboardName)

    logInserted()
    logEnd()
  }

}