package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class GtPstnClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_K7M_gt_pstnIN = s"${Stg0Schema}.crm_S_ACCNT_POSTN"
  val Nodet_team_k7m_aux_d_K7M_gt_pstnOUT = s"${DevSchema}.K7M_gt_pstn"
  val dashboardPath = s"${config.auxPath}K7M_gt_pstn"


  override val dashboardName: String = Nodet_team_k7m_aux_d_K7M_gt_pstnOUT //витрина
  override def processName: String = SparkMainClass.applicationName

  def DoGtPstn() {

    Logger.getLogger("org").setLevel(Level.WARN)

    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""SELECT
          pstn.OU_EXT_ID
          ,pstn.POSITION_ID
          ,pstn.CVRG_ROLE_CD
          ,row_number() over(partition by pstn.OU_EXT_ID,upper(pstn.CVRG_ROLE_CD) order by pstn.CREATED desc) as rn
      FROM $Node1t_team_k7m_aux_d_K7M_gt_pstnIN pstn
      WHERE upper(pstn.CVRG_ROLE_CD) in (${SparkMainClass.CvrgRoleCd})
        """
    ) .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_gt_pstnOUT")

    logInserted()
    logEnd()
  }
}
