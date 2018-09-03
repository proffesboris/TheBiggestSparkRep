package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CrmCustTeamClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_K7M_CRM_CUST_TEAMIN = s"${DevSchema}.K7M_gt_pstn"
  val Nodet_team_k7m_aux_d_K7M_CRM_CUST_TEAMOUT = s"${DevSchema}.K7M_CRM_CUST_TEAM"
  val dashboardPath = s"${config.auxPath}K7M_CRM_CUST_TEAM"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_CRM_CUST_TEAMOUT //витрина
  override def processName: String = SparkMainClass.applicationName

  def DoCrmCustTeam() {

    Logger.getLogger("org").setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""SELECT
          pstn.OU_EXT_ID                             as CUST_ID          --ID организации--
          ,pstn.POSITION_ID                           as POSITION_ID      --ID позиции--
          ,pstn.CVRG_ROLE_CD                          as ROLE             --Роль в команде--
          ,'N'                                        as IS_MAIN          --Флаг Главный--
      FROM $Node1t_team_k7m_aux_d_K7M_CRM_CUST_TEAMIN pstn
     WHERE pstn.rn=1
        """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_CUST_TEAMOUT")

    logInserted()
    logEnd()
  }
}
