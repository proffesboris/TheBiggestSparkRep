package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmSystemClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_systemsIN = s"${Stg0Schema}.mdm_contequiv"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_systemsIN = s"${DevSchema}.tmp_clf_mdm_filter"
  val Node3t_team_k7m_aux_d_tmp_clf_mdm_systemsIN = s"${Stg0Schema}.mdm_cdadminsystp"
  val  Nodet_team_k7m_aux_d_tmp_clf_mdm_systemsOUT = s"${DevSchema}.tmp_clf_mdm_systems"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_systems"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_systemsOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmSystem()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_systemsOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""		select
         		  t.cont_id
         		, t.admin_client_id
         		, tp.name
         		, tp.description
         	from
         		$Node2t_team_k7m_aux_d_tmp_clf_mdm_systemsIN filter
         		join $Node1t_team_k7m_aux_d_tmp_clf_mdm_systemsIN t
         			on t.cont_id = filter.cont_id
         		join (select name,description,admin_sys_tp_cd from $Node3t_team_k7m_aux_d_tmp_clf_mdm_systemsIN where lang_tp_cd=2200) tp
         			on tp.admin_sys_tp_cd = t.admin_sys_tp_cd
         	where 1 = 1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_systemsOUT")

    logInserted()

  }
}