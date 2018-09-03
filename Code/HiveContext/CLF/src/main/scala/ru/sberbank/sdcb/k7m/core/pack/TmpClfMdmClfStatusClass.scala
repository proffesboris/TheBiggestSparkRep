package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmClfStatusClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_clf_statusIN = s"${Stg0Schema}.mdm_cdclientsttp"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_clf_statusOUT = s"${DevSchema}.tmp_clf_mdm_clf_status"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_clf_status"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_clf_statusOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmClfStatus()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_clf_statusOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""select
         		  t.client_st_tp_cd
         		, t.name
         	from $Node1t_team_k7m_aux_d_tmp_clf_mdm_clf_statusIN t
         	where lang_tp_cd = 2200
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_clf_statusOUT")

    logInserted()
  }
}

