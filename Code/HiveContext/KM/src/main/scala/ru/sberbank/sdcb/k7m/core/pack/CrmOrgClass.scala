package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CrmOrgClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_K7M_crm_orgIN = s"${Stg0Schema}.crm_S_ORG_EXT"
  val Node2t_team_k7m_aux_d_K7M_crm_orgIN = s"${Stg0Schema}.crm_S_ORG_EXT_X"
  val Nodet_team_k7m_aux_d_K7M_crm_orgOUT = s"${DevSchema}.K7M_crm_org"
  val dashboardPath = s"${config.auxPath}K7M_crm_org"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_crm_orgOUT //витрина
  override def processName: String = SparkMainClass.applicationName

  def DoCrmOrg() {

    Logger.getLogger(Nodet_team_k7m_aux_d_K7M_crm_orgOUT).setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""SELECT
     org.ROW_ID        as id
     , org_x.sbrf_inn    as inn
	   , org_x.sbrf_kio    as kio
     , org_x.sbrf_kpp	   as kpp
	   , org.name		   as FULL_NAME
	   , org.ou_type_cd    as type_cd
	   , org_x.attrib_39   as legal_form_cd
	   , org_x.attrib_46   as ogrn
	   , org_x.attrib_68   as OKPO
	   , org.pr_postn_id   as pos_vko_id
	   , org.x_osb_div_id  as terr_id
  FROM $Node1t_team_k7m_aux_d_K7M_crm_orgIN org
  join $Node2t_team_k7m_aux_d_K7M_crm_orgIN org_x
    on org.ROW_ID = org_x.PAR_ROW_ID
 where org.INT_org_FLG='N'
   and lower(org.OU_TYPE_CD)<>${SparkMainClass.OrgOuTypeCd}
   and lower(org.cust_stat_cd)<>${SparkMainClass.OrgCustStatCd}
        """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_crm_orgOUT")

    logInserted()
    logEnd()
  }
}
