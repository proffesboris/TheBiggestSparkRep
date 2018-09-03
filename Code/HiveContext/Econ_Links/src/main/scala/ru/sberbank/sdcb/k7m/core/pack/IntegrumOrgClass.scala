package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class IntegrumOrgClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_integrum_orgIN = s"${Stg0Schema}.int_ul_organization_rosstat"
  val Nodet_team_k7m_aux_d_k7m_integrum_orgOUT = s"${DevSchema}.k7m_integrum_org"
  val dashboardPath = s"${config.auxPath}k7m_integrum_org"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_integrum_orgOUT //витрина
  override def processName: String = "EL"

  def DoIntOrg(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_integrum_orgOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s""" select distinct
             ul_inn,
             ul_org_id,
             ul_branch_cnt,
             ul_okopf_cd
         from  (
             select
                 ul_inn,
                 ul_org_id,
                 ul_branch_cnt,
                 ul_okopf_cd,
                 count(*) over (partition by s.ul_inn) cnt
             from $Node1t_team_k7m_aux_d_k7m_integrum_orgIN    s
             where  effectivefrom < '${dateString}'
                and effectiveto >= '${dateString}'
                 and egrul_org_id is not null
                 and ul_inn is not null
             ) s
         where cnt=1 or cast(ul_branch_cnt as  bigint)>=1
      """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_integrum_orgOUT")

    logEnd()

  }
}
