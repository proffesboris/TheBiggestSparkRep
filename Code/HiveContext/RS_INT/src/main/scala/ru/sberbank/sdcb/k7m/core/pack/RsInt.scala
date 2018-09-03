package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

class RsInt(override val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  override val processName = "RS_INT"
  var dashboardName = s"${config.aux}.proxyrs_int_final"

  private val bDate = LoggerUtils.obtainRunDtWithoutTime(spark, config.aux)

  def run(): Unit = {
    logStart()

    spark.sql(s"drop table if exists ${config.aux}.proxyrs_int_final")

    spark.sql(s"""create table ${config.aux}.proxyrs_int_final as
    select
        src.u7m_id,
        src.RISK_SEGMENT_OFFLINE,
        src.RISK_SEGMENT_CRM,
        src.RISK_SEGMENT_CRM_DT,
        src.RISK_SEGMENT,
        rdm.k7m_flag
      from
        (
          select
              b.u7m_id,
              a.RISK_SEGMENT_OFFLINE,
              c.x_risk_cat as RISK_SEGMENT_CRM,
              c.x_risk_cat_dt as RISK_SEGMENT_CRM_DT,
              CASE
                WHEN c.x_risk_cat is not NULL and (d.k7m_old_flag = '1' or add_months(c.x_risk_cat_dt, 12) > '$bDate') THEN c.x_risk_cat
                ELSE a.RISK_SEGMENT_OFFLINE
              END as RISK_SEGMENT
            from
              ${config.pa}.clu b
              left join ${config.pa}.proxyrisksegmentstandalone a on a.u7m_id = b.u7m_id
              left join ${config.stg}.crm_s_org_ext_x c on b.crm_id = c.par_row_id
              left join internal_rdm.rdm_risk_segment_k7m_mast_v d on c.x_risk_cat = d.nm
        ) src
        left join internal_rdm.rdm_risk_segment_k7m_mast_v rdm on src.RISK_SEGMENT = rdm.nm
    """.stripMargin)

    logInserted()

    logEnd()
  }

}