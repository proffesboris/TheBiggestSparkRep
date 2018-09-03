package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmClfNameClass  (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_clf_nameIN = s"${Stg0Schema}.mdm_personname"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_clf_nameIN = s"${DevSchema}.tmp_clf_mdm_filter"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_clf_nameOUT = s"${DevSchema}.tmp_clf_mdm_clf_name"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_clf_name"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_clf_nameOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmClfName()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_clf_nameOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""	select cont_id, clf_l_name, clf_f_name, clf_m_name
         	from
         		(select
         			  t.cont_id
         			, upper(trim(t.last_name))		clf_l_name
         			, upper(trim(t.given_name_one))	clf_f_name
         			, upper(trim(t.given_name_two))	clf_m_name
         			, row_number() over( partition by t.cont_id
         				order by
         					  t.name_usage_tp_cd
         					, nvl(t.end_dt,cast('9999-12-31 00:00:00' as timestamp)) desc
         					, t.start_dt desc
         				) rn
         		from
         			$Node1t_team_k7m_aux_d_tmp_clf_mdm_clf_nameIN t
         			join $Node2t_team_k7m_aux_d_tmp_clf_mdm_clf_nameIN filter
         				on filter.cont_id = t.cont_id
         		) t
         	where rn=1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_clf_nameOUT")

    logInserted()
  }
}