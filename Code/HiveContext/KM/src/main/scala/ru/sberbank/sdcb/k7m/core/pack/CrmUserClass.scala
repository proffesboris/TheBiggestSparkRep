package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CrmUserClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_K7M_crm_USERIN = s"${Stg0Schema}.crm_S_CONTACT"
  val Nodet_team_k7m_aux_d_K7M_crm_USEROUT = s"${DevSchema}.K7M_CRM_USER"
  val dashboardPath = s"${config.auxPath}K7M_CRM_USER"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_crm_USEROUT //витрина
  override def processName: String = SparkMainClass.applicationName


  def DoCrmUser() {

    Logger.getLogger("org").setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""SELECT
       cn.bu_id                       as org_id
      ,cn.row_id                      as id
      ,cn.email_addr                  as EMAIL
      ,cn.job_title                   as post
      ,cn.fst_name                    as F_NAME
      ,cn.last_name                   as S_NAME
      ,cn.mid_name                    as L_NAME
      ,cn.emp_num                     as emp_num
      ,cn.work_ph_num                 as W_PHONE
      ,cn.cell_ph_num                 as M_PHONE
      ,cn.birth_dt                    as b_date
  FROM $Node1t_team_k7m_aux_d_K7M_crm_USERIN cn
 WHERE cn.EMP_FLG = 'Y'
        """
    )
       .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_crm_USEROUT")

    logInserted()
    logEnd()
  }
}
