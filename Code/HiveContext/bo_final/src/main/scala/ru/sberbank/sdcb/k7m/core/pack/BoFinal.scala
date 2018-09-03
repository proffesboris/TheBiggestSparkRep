package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{SaveMode, SparkSession}

class BoFinal(override val spark: SparkSession, val config: Config) extends EtlJob with EtlLogger {



  override val processName = "BO_FINAL"
  var dashboardName = s"${config.pa}.bo"

  val tblBoGen = s"${config.aux}.bo_gen"
  val tblBoBBO = s"${config.aux}.bo_bbo"
  val tblBoLGD = s"${config.aux}.bo_lgd"
  val tblBoSKR = s"${config.aux}.bo_skr"

  def run() {

    logStart()

    dashboardName = s"${config.aux}.bo_incomplete"

    Seq(tblBoGen, tblBoBBO, tblBoLGD, tblBoSKR)
      .map(spark.table(_))
      .reduceLeft(_.union(_))
      .cache()
      .createOrReplaceTempView("bo")

    spark.sql(s"drop table if exists ${config.aux}.bo_incomplete")

    spark.sql(s"""create table ${config.aux}.bo_incomplete
                |stored as parquet
                |as
                |select 
                |    core.bo_id,
                |    core.u7m_id,
                |    core.key
                |from (
                |    select 
                |        k.bo_id,
                |        k.u7m_id,
                |        s.key
                |    from  ${config.pa}.bo_keys k
                |        cross join ${config.aux}.dict_mandatory_keys_out s
                |    where s.obj = 'BO'    
                |    ) core 
                |    left join bo
                |    on core.bo_id =bo.bo_id
                |        and core.u7m_id =bo.u7m_id
                |        and core.key =bo.key
                |where bo.value_c is null and bo.value_n is null and bo.value_d is null
                |union all   --21/07/2018 SDCB-461 фильтрация ВО Овердрафтов, если по счету не посчитан лимит овердрафта в ЕКС
                |select distinct
                |     bo.bo_id,
                |     bo.u7m_id,
                |     'NO_FLAG_OVER'
                |from bo
                |   inner join (select u7m_id, max(flag_over) as flag_over
                |                 from ( select u7m_id, flag_over, count(flag_over)  as cnt
                |                        from ${config.pa}.acc
                |                        group by u7m_id, flag_over
                |                       ) t
                |                group by u7m_id
                |               ) acc_over on bo.u7m_id = acc_over.u7m_id
                |where bo.clust_id = 'OVERDRAFT'
                |and acc_over.flag_over = 0
                |union all   --21/07/2018 SDCB-461 фильтрация ВО Овердрафтов, если нижняя граница ВО EXP_L превышает рассчитанный клиенту лимит овердрафта в ЕКС
                |select
                |     bo_exp.bo_id,
                |     bo_exp.u7m_id,
                |     'LIMIT_OVER'
                |from ${config.pa}.acc acc
                |inner join (select
                |              u7m_id,
                |              bo_id,
                |              max(EXP_L) as EXP_L,
                |              max(EXP_U) as EXP_U
                |            from (select
                |                     u7m_id,
                |                     bo_id,
                |                     case when key = 'EXP_L' then value_n end as EXP_L,
                |                     case when key = 'EXP_U' then value_n end as EXP_U
                |                     from bo
                |                     where clust_id = 'OVERDRAFT'
                |                     and key in ('EXP_L', 'EXP_U')) t
                |             group by u7m_id, bo_id
                |             ) bo_exp on bo_exp.u7m_id = acc.u7m_id
                |where acc.flag_over = 1
                |and bo_exp.EXP_L > acc.ovr_limit_sum
                |""".stripMargin)

    logInserted()

    dashboardName = s"${config.pa}.bo"

    spark.sql(s"""select *
                |from bo b
                |where   not exists (
                |    select bo_id,u7m_id
                |    from ${config.aux}.bo_incomplete bi
                |    where bi.bo_id=b.bo_id
                |        and bi.u7m_id = b.u7m_id
                |    )""".stripMargin)
      .write
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.paPath}bo")
      .format("parquet")
      .saveAsTable(dashboardName)

    logInserted()

    logEnd()
  }
}