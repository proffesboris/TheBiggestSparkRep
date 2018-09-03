package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmClfClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_clfIN = s"${DevSchema}.tmp_clf_mdm_filter"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_clfIN = s"${Stg0Schema}.mdm_contact"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_clfOUT = s"${DevSchema}.tmp_clf_mdm_clf"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_clf"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_clfOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmClf()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_clfOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""		select
         		  t.cont_id
         		, t.contact_name
         		, t.inactivated_dt
         		, t.client_st_tp_cd
         		, t.last_update_dt
         	from
         		$Node2t_team_k7m_aux_d_tmp_clf_mdm_clfIN t
         		join $Node1t_team_k7m_aux_d_tmp_clf_mdm_clfIN filter
         			on filter.cont_id = t.cont_id
         	where person_org_code='P'
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_clfOUT")

    logInserted()
   }
}
