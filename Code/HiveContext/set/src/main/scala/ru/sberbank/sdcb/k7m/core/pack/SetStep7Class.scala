package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


class SetStep7Class(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa


    val tbl_set_cred_active_IN = s"${DevSchema}.set_cred_active"
    val tbl_int_fin_stmt_rsbu_IN = s"${DevSchema}.int_fin_stmt_rsbu"
    val tbl_k7m_crm_org_mj_IN = s"${DevSchema}.k7m_crm_org_mj"
    val tbl_set_fok_fin_stmt_rsbu_IN = s"${DevSchema}.SET_fok_fin_stmt_rsbu"


    val tbl_set_cred_rep_IN = s"${DevSchema}.set_cred_rep"
    val tbl_set_cred_rep1d_OUT = s"${DevSchema}.set_cred_rep1d"
    val dashboardPath1 = s"${config.auxPath}set_cred_rep1d"
    val tbl_set_cred_rep2d_OUT = s"${DevSchema}.set_cred_rep2d"
    val dashboardPath2 = s"${config.auxPath}set_cred_rep2d"
    val tbl_set_cred_enriched_OUT = s"${DevSchema}.set_cred_enriched"
    val dashboardPath = s"${config.auxPath}set_cred_enriched"


    var dashboardName: String = tbl_set_cred_rep1d_OUT //витрина
    override def processName: String = "SET"

    def DoSetStep7() {

      Logger.getLogger("step7").setLevel(Level.WARN)
      //STEP 5 Проведем отсев проводок и отсеем договора так, чтобы существовали транзакции выдачи или гашения по ОД

      //отберем транзакции гашения
      logStart()

      dashboardName = tbl_set_cred_rep1d_OUT
      val createHiveTable1 = spark.sql(s"""
select
    cred.pr_cred_id,
    cred.inn,
    cred.ndog,
    cred.ddog,
    min(integ.fin_stmt_start_dt) as int_md
from
    $tbl_set_cred_active_IN as cred --set_cred_rating
    inner join $tbl_int_fin_stmt_rsbu_IN  as integ
    on integ.cust_inn_num = cred.inn
where  add_months(integ.fin_stmt_start_dt, -6) <= cred.ddog
   and integ.fin_stmt_2110_amt is not null and integ.fin_stmt_2110_amt>0
group by  cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog
""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath1)
        .saveAsTable(s"$tbl_set_cred_rep1d_OUT")
        logInserted()

      //SET 5-2 отберем транзакции выдачи
      dashboardName = tbl_set_cred_rep2d_OUT
      val createHiveTable2 = spark.sql(s"""
select
    cred.pr_cred_id,
    cred.inn,
    cred.ndog,
    cred.ddog,
    min(fok.fin_stmt_start_dt) as fok_md
from
     $tbl_set_cred_active_IN as cred --set_cred_rating
     inner join
     $tbl_k7m_crm_org_mj_IN mj
     on mj.inn = cred.inn
     join $tbl_set_fok_fin_stmt_rsbu_IN as fok
      on fok.crm_cust_id = mj.id
where  add_months(fok.fin_stmt_start_dt, -6) <= cred.ddog
    and fok.fin_stmt_2110_amt_year>0
group by  cred.pr_cred_id, cred.inn, cred.ndog, cred.ddog
""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath2)
        .saveAsTable(s"$tbl_set_cred_rep2d_OUT")
      logInserted()

      dashboardName = tbl_set_cred_enriched_OUT
      //отберем транзакции списания
      val createHiveTable3 = spark.sql(s"""
select
    cred.*,
    coalesce(pastinteg.int_revenue,nextinteg.int_revenue,nextinteg2.int_revenue) as int_revenue,
    coalesce(pastfok.fok_revenue  ,nextfok.fok_revenue ,nextfok2.fok_revenue)   as fok_revenue,
    months_between(cred.ddog, coalesce(fok_md,int_md))    as report_month_ago,
    row_number() over (partition by cred.inn, cred.instrument order by cred.ddog desc) as num_cr_desc
from
    $tbl_set_cred_active_IN as cred --set_cred_rating
    left join ${tbl_set_cred_rep_IN}1 as pastinteg on pastinteg.pr_cred_id = cred.pr_cred_id
    left join ${tbl_set_cred_rep_IN}2 as nextinteg on nextinteg.pr_cred_id = cred.pr_cred_id
    left join ${tbl_set_cred_rep_IN}3 as nextinteg2 on nextinteg2.pr_cred_id = cred.pr_cred_id

    left join ${tbl_set_cred_rep_IN}4 as pastfok on pastfok.pr_cred_id = cred.pr_cred_id
    left join ${tbl_set_cred_rep_IN}5 as nextfok on nextfok.pr_cred_id = cred.pr_cred_id
    left join ${tbl_set_cred_rep_IN}6 as nextfok2 on nextfok2.pr_cred_id = cred.pr_cred_id

    left join $tbl_set_cred_rep1d_OUT as integmd on integmd.pr_cred_id = cred.pr_cred_id
    left join $tbl_set_cred_rep2d_OUT as fokmd on fokmd.pr_cred_id = cred.pr_cred_id""")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .option("path", dashboardPath)
        .saveAsTable(s"$tbl_set_cred_enriched_OUT")
      logInserted()
      logEnd()
    }
}

