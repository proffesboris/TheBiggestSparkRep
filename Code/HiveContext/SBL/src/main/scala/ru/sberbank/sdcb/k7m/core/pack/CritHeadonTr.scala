package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql.{SaveMode,SparkSession}

/**
  * Витрина SBL 6. Встречные транзакции
  */
class CritHeadonTr(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  import SparkMain._
  val auxSchema = config.aux




  override val dashboardName: String = s"$auxSchema.crit_headon_tr"
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_headon_tr"

  def run(date: LocalDate): Unit = {
    execute(date.minusYears(1).toString, date.toString)
  }

  def execute(date_cond_from: String, date_cond_to: String): Unit = {
    logStart()
    var t_temp_ktsum_df = spark.sql(s"select '' as inn_dt, '' as inn_kt, '' as dt, '' as sum_tr_kt from $auxSchema.basis_client where (1 <> 1)")
    var t_temp_dtsum_df = spark.sql(s"select '' as inn_dt, '' as inn_kt, '' as dt, '' as sum_tr_dt from $auxSchema.basis_client where (1 <> 1)")

    var sql_query =
      s"""
    select
            case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn_dt
           ,case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn_kt
           ,'$date_cond_to' as dt
           ,sum(v.c_sum_nt) as sum_tr_kt
           from $auxSchema.trbasis_kras v
                where
           ( (case when v.ktdt = 0 then v.inn_st else v.inn_sec end) in (select org_inn_crm_num inn from $auxSchema.basis_client) )
                    and (v.c_date_prov >= '$date_cond_from')
                    and (v.c_date_prov < '$date_cond_to')
                    and v.inn_st  <> '$SBRF_INN'
                    and v.inn_sec <> '$SBRF_INN'
                    and (filt = 0)

    group by (case when v.ktdt = 0 then v.inn_sec else v.inn_st end), (case when v.ktdt = 0 then v.inn_st else v.inn_sec end)"""
    t_temp_ktsum_df = t_temp_ktsum_df.union(spark.sql(sql_query))

    sql_query =
      s"""
    select
            case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn_dt
           ,case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn_kt
           ,'$date_cond_to' as dt
           ,sum(v.c_sum_nt) as sum_tr_dt


           from $auxSchema.trbasis_kras v
                where
           ( (case when v.ktdt = 0 then v.inn_sec else v.inn_st end) in (select org_inn_crm_num inn from $auxSchema.basis_client) )
                    and (v.c_date_prov >= '$date_cond_from')
                    and (v.c_date_prov < '$date_cond_to')
                    and v.inn_st  <> '$SBRF_INN'
                    and v.inn_sec <> '$SBRF_INN'
                    and (filt = 0)

    group by (case when v.ktdt = 0 then v.inn_sec else v.inn_st end), (case when v.ktdt = 0 then v.inn_st else v.inn_sec end)"""
    t_temp_dtsum_df = t_temp_dtsum_df.union(spark.sql(sql_query))

    t_temp_ktsum_df.persist().count()
    t_temp_dtsum_df.persist().count()

    t_temp_ktsum_df.createOrReplaceTempView("t_temp_ktsum")
    t_temp_dtsum_df.createOrReplaceTempView("t_temp_dtsum")

    sql_query =
      """
    select
           kt1.inn_dt,
           kt1.inn_kt,
           kt1.dt,
           kt1.sum_tr_kt,
           dt1.sum_tr_dt

           from
           t_temp_ktsum kt1
           inner join
           t_temp_dtsum dt1

           on (kt1.inn_dt = dt1.inn_kt) and (kt1.inn_kt = dt1.inn_dt)"""
    val t_temp_headon_tr_df = spark.sql(sql_query)
    t_temp_headon_tr_df
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path",  s"${config.auxPath}t_temp_headon_tr")
      .saveAsTable(s"$auxSchema.t_temp_headon_tr")
    //t_temp_headon_tr_df = spark.sql(s"select * from $auxSchema.t_temp_headon_tr_df")
    //t_temp_headon_tr_df.createOrReplaceTempView("t_temp_headon_tr")

    sql_query =
      s"""
    select
           tr.inn_dt,
           tr.inn_kt,
           tr.sum_tr_kt,
           tr.sum_tr_dt,
           tr.dt,
           a.revenue_L12M_true as dt_revenue_L12M,
           b.revenue_L12M_true as kt_revenue_L12M

           from
           $auxSchema.t_temp_headon_tr tr
           left join
           $auxSchema.Revenue_target a
           on tr.inn_dt = a.inn --and tr.dt = a.dt
           left join
           $auxSchema.Revenue_target b
           on tr.inn_kt = b.inn --and tr.dt = b.dt

           where ((a.revenue_L12M_true  >1) or (b.revenue_L12M_true > 1))
           and (tr.inn_dt <> tr.inn_kt)
           and (tr.inn_dt not in ('0', '0000000000'))
           and (tr.inn_kt not in ('0', '0000000000'))"""

    val t_temp_headon_tr_ob_df = spark.sql(sql_query)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path",  s"${config.auxPath}t_temp_headon_tr_ob")
      .saveAsTable(s"$auxSchema.t_temp_headon_tr_ob")
    //t_temp_headon_tr_ob_df.createOrReplaceTempView("t_temp_headon_tr_ob")

    sql_query =
      s"""
select
          inn_dt as inn1,
          inn_kt as inn2,
          dt,
          (2* k_dt * k_kt) / ( k_dt + k_kt) as quantity
    from
    (
        select
            inn_dt,
            inn_kt,
            dt,
            case when (dt_revenue_L12M is not null) then sum_tr_dt / dt_revenue_L12M
            else  sum_tr_dt / kt_revenue_L12M end as k_dt,
            case when (kt_revenue_L12M is not null) then sum_tr_kt / kt_revenue_L12M
            else sum_tr_kt / dt_revenue_L12M end as k_kt

        from
         $auxSchema.t_temp_headon_tr_ob
        where
            ((case when (dt_revenue_L12M is not null) then sum_tr_dt / dt_revenue_L12M
                else  sum_tr_dt / kt_revenue_L12M end) > $CONST_COND_DOWN) and
            ((case when (kt_revenue_L12M is not null) then sum_tr_kt / kt_revenue_L12M
                else sum_tr_kt / dt_revenue_L12M end) > $CONST_COND_DOWN)
            and inn_dt <> inn_kt
    )"""
    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath.concat("_temp"))
      .saveAsTable(dashboardName.concat("_temp"))



    sql_query = s"""select distinct inn1, inn2, dt, quantity
                       from
                       (
    select inn1, inn2, dt, quantity
    from
    (
      select row_number() over (partition by a.inn2, a.dt order by a.quantity desc) as rn
    , a.inn2
    , a.dt
    , a.inn1
    , a.quantity
    from ${dashboardName.concat("_temp")} a
    join $auxSchema.basis_client b
      on a.inn2 = b.org_inn_crm_num
    ) d
    where d.rn <= $CONST_TOP_HEADONTR_COUNT
    union all
    select inn1, inn2, dt, quantity
    from
    (
      select row_number() over (partition by inn1, dt order by quantity desc) as rn
      , inn1
      , dt
      , inn2
      , quantity
      from ${dashboardName.concat("_temp")} a
      join $auxSchema.basis_client b
        on a.inn1 = b.org_inn_crm_num
    ) d
    where d.rn <= $CONST_TOP_HEADONTR_COUNT
    )
      """

    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(dashboardName)

    logInserted()
    logEnd()
  }
}
