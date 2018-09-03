package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmPersClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_persIN = s"${Stg0Schema}.mdm_person"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_persIN = s"${DevSchema}.tmp_clf_mdm_filter"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_persOUT = s"${DevSchema}.tmp_clf_mdm_pers"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_pers"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_persOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmPers()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_persOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""		select
         		  t.cont_id
         		, t.gender_tp_code
         		, t.birth_dt
         		, t.sb_birth_place
         		, t.deceased_dt
         		, t.citizenship_tp_cd
         	from
         		$Node1t_team_k7m_aux_d_tmp_clf_mdm_persIN t
         		join $Node2t_team_k7m_aux_d_tmp_clf_mdm_persIN filter
         			on filter.cont_id = t.cont_id
         	where 1=1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_persOUT")

    logInserted()
  }
}