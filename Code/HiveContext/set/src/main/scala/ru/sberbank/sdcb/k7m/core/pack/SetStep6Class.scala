package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetStep6Class(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val tbl_set_cred_active_IN = s"${DevSchema}.set_cred_active"
    val tbl_int_fin_stmt_rsbu_IN = s"${DevSchema}.int_fin_stmt_rsbu"
    val tbl_k7m_crm_org_mj_IN = s"${DevSchema}.k7m_crm_org_mj"
    val tbl_fok_fin_stmt_rsbu_IN = s"${DevSchema}.fok_fin_stmt_rsbu"

    val tbl_set_fok_fin_stmt_rsbu = "SET_fok_fin_stmt_rsbu";
    val tbl_set_fok_fin_stmt_rsbu_INOUT = s"${DevSchema}.$tbl_set_fok_fin_stmt_rsbu"
    val tbl_set_fok_fin_stmt_rsbu_PATH = s"${config.auxPath}$tbl_set_fok_fin_stmt_rsbu"

    val tbl_set_cred_rep = "set_cred_rep";
    val tbl_set_cred_rep_OUT = s"${DevSchema}.$tbl_set_cred_rep"
    val tbl_set_cred_rep_PATH = s"${config.auxPath}${tbl_set_cred_rep}"


    var dashboardName: String = tbl_set_cred_rep_OUT //витрина
    override def processName: String = "SET"

    def DoSetStep6() {

      Logger.getLogger("step6").setLevel(Level.WARN)
      //STEP 6. Добавим отчетность

      //честная (за 10 месяцев до договора) отчетность из интегрума-
      logStart()

      dashboardName = s"${tbl_set_cred_rep_OUT}1"

      val createHiveTable1 = spark.sql(s"""
select
    pr_cred_id,
    inn,
    ndog,
    ddog,
    int_revenue,
    integrum_period_start_dt
from (
    select
        cred.pr_cred_id,
        cred.inn,
        cred.ndog,
        cred.ddog,
        integ.fin_stmt_start_dt as integrum_period_start_dt,
        integ.fin_stmt_2110_amt as int_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by integ.fin_stmt_start_dt desc) as rn
   from
        $tbl_set_cred_active_IN as cred --set_cred_active
        inner join
        $tbl_int_fin_stmt_rsbu_IN as integ
        on integ.cust_inn_num = cred.inn
where  add_months(integ.fin_stmt_start_dt, 10) <= ddog
   and ddog < add_months(integ.fin_stmt_start_dt, 22)
   and integ.fin_stmt_2110_amt is not null and integ.fin_stmt_2110_amt>0
) x
where rn=1
""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", s"${tbl_set_cred_rep_PATH}1")
        .saveAsTable(s"${tbl_set_cred_rep_OUT}1")
      logInserted()

      dashboardName = s"${tbl_set_cred_rep_OUT}2"
      //не очень честная (за 0 месяцев до договора) отчетность из интегрума
      val createHiveTable2 = spark.sql(s"""
select
    pr_cred_id,
    inn,
    ndog,
    ddog,
    int_revenue,
    integrum_period_start_dt
from (
    select
        cred.pr_cred_id,
        cred.inn,
        cred.ndog,
        cred.ddog,
        integ.fin_stmt_start_dt as integrum_period_start_dt,
        integ.fin_stmt_2110_amt as int_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by integ.fin_stmt_start_dt desc) as rn
   from
        $tbl_set_cred_active_IN as cred --set_cred_rating
        inner join
        $tbl_int_fin_stmt_rsbu_IN as integ
        on integ.cust_inn_num = cred.inn
where  add_months(integ.fin_stmt_start_dt, 0) <= ddog
   and ddog < add_months(integ.fin_stmt_start_dt, 12)
   and integ.fin_stmt_2110_amt is not null and integ.fin_stmt_2110_amt>0
) x
where rn=1
""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", s"${tbl_set_cred_rep_PATH}2")
        .saveAsTable(s"${tbl_set_cred_rep_OUT}2")

      logInserted()

      dashboardName = s"${tbl_set_cred_rep_OUT}3"

      //не очень честная (за 0 месяцев до договора) отчетность из интегрума
      val createHiveTable3 = spark.sql(s"""
select
    pr_cred_id,
    inn,
    ndog,
    ddog,
    int_revenue,
    integrum_period_start_dt
from (
    select
        cred.pr_cred_id,
        cred.inn,
        cred.ndog,
        cred.ddog,
        integ.fin_stmt_start_dt as integrum_period_start_dt,
        integ.fin_stmt_2110_amt as int_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by integ.fin_stmt_start_dt desc) as rn
   from
        $tbl_set_cred_active_IN as cred --set_cred_rating
        inner join
        $tbl_int_fin_stmt_rsbu_IN as integ
        on integ.cust_inn_num = cred.inn
where  add_months(integ.fin_stmt_start_dt, -6) <= ddog
   and ddog < add_months(integ.fin_stmt_start_dt, 6)
   and integ.fin_stmt_2110_amt is not null and integ.fin_stmt_2110_amt>0
) x
where rn=1""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", s"${tbl_set_cred_rep_PATH}3")
        .saveAsTable(s"${tbl_set_cred_rep_OUT}3")
      logInserted()

      //Подготовить отчетность за год по ФОК.
      val createHiveTable_FOK_YR =
      spark.sql("""
          select
          crm_cust_id,
             fin_stmt_year ,
             fin_stmt_period,
             fin_stmt_start_dt,
             fin_stmt_end_dt,
             fin_stmt_2110_amt as fin_stmt_2110_amt_RUNNING_TOTAL,
             fin_stmt_2110_amt_clear,
             sum(fin_stmt_2110_amt_clear) over (
                  partition by
                      crm_cust_id
                  order by unix_timestamp(fin_stmt_start_dt)
                  range between   30240000 PRECEDING and current row
                  ) fin_stmt_2110_amt_year
          from (
          select
             crm_cust_id,
             fin_stmt_year ,
             fin_stmt_period,
             fin_stmt_start_dt,
             fin_stmt_end_dt,
             fin_stmt_2110_amt,
             coalesce(fin_stmt_2110_amt,0)- lag(coalesce(fin_stmt_2110_amt,0),1,0) over (
                  partition by
                      crm_cust_id,fin_stmt_year
                 order by fin_stmt_start_dt) fin_stmt_2110_amt_clear
          from custom_cb_k7m_aux.fok_fin_stmt_rsbu
          where fin_stmt_2110_amt>0
          ) x
        """
      ) .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", s"${tbl_set_fok_fin_stmt_rsbu_PATH}")
        .saveAsTable(s"${tbl_set_fok_fin_stmt_rsbu_INOUT}")
      logInserted()

      dashboardName = s"${tbl_set_cred_rep_OUT}4"
      // честная (за 6 месяцев до договора) отчетность из фок
      val createHiveTable4 = spark.sql(s"""
select
     pr_cred_id,
     inn,
     ndog,
     ddog,
     fok_period_start_dt,
     fok_revenue
from (
    select
        cred.pr_cred_id,
        cred.inn,
        cred.ndog,
        cred.ddog,
        fok.fin_stmt_start_dt as fok_period_start_dt,
        fok.fin_stmt_2110_amt_year as fok_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by fok.fin_stmt_start_dt desc) as rn
   from
        $tbl_set_cred_active_IN  as cred -- set_cred_rating
        inner join
        $tbl_k7m_crm_org_mj_IN mj
        on mj.inn = cred.inn
        join $tbl_set_fok_fin_stmt_rsbu_INOUT as fok
        on fok.crm_cust_id = mj.id
where  add_months(fok.fin_stmt_start_dt, 6) <= ddog
   and ddog < add_months(fok.fin_stmt_start_dt, 18)
   and fok.fin_stmt_2110_amt_year>0
) x
where rn=1""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", s"$tbl_set_cred_rep_PATH}4")
        .saveAsTable(s"${tbl_set_cred_rep_OUT}4")

      logInserted()

      dashboardName = s"${tbl_set_cred_rep_OUT}5"

      // почти честная (за 0 месяцев до договора) отчетность из фок
      val createHiveTable5 = spark.sql(s"""
select
     pr_cred_id,
     inn,
     ndog,
     ddog,
     fok_period_start_dt,
     fok_revenue
from (
    select
        cred.pr_cred_id,
        cred.inn,
        cred.ndog,
        cred.ddog,
        fok.fin_stmt_start_dt as fok_period_start_dt,
        fok.fin_stmt_2110_amt_year as fok_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by fok.fin_stmt_start_dt desc) as rn
   from
        $tbl_set_cred_active_IN  as cred -- set_cred_rating
        inner join
        $tbl_k7m_crm_org_mj_IN mj
        on mj.inn = cred.inn
        join $tbl_set_fok_fin_stmt_rsbu_INOUT as fok
        on fok.crm_cust_id = mj.id
where  add_months(fok.fin_stmt_start_dt, 0) <= ddog
   and ddog < add_months(fok.fin_stmt_start_dt, 12)
   and fok.fin_stmt_2110_amt_year>0
) x
where rn=1""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", s"${tbl_set_cred_rep_PATH}5")
        .saveAsTable(s"${tbl_set_cred_rep_OUT}5")

      logInserted()

      dashboardName = s"${tbl_set_cred_rep_OUT}6"

      // совсем не честная (до 6 месяцев после договора) отчетность из фок
      val createHiveTable6 = spark.sql(s"""
select
     pr_cred_id,
     inn,
     ndog,
     ddog,
     fok_period_start_dt,
     fok_revenue
from (
    select
        cred.pr_cred_id,
        cred.inn,
        cred.ndog,
        cred.ddog,
        fok.fin_stmt_start_dt as fok_period_start_dt,
        fok.fin_stmt_2110_amt_year as fok_revenue,
        row_number() over (partition by cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog order by fok.fin_stmt_start_dt desc) as rn
   from
        $tbl_set_cred_active_IN as cred --set_cred_rating
        inner join
        $tbl_k7m_crm_org_mj_IN mj
        on mj.inn = cred.inn
        join $tbl_set_fok_fin_stmt_rsbu_INOUT as fok
        on fok.crm_cust_id = mj.id
where  add_months(fok.fin_stmt_start_dt, -6) <= ddog
   and fok.fin_stmt_2110_amt_year>0
) x
where rn=1""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", s"${tbl_set_cred_rep_PATH}6")
        .saveAsTable(s"${tbl_set_cred_rep_OUT}6")

      logInserted()
      logEnd()
    }
}

