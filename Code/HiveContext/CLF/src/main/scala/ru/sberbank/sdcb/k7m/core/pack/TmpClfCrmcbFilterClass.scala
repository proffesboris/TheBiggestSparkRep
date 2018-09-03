package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfCrmcbFilterClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_crmcb_filterIN = s"${DevSchema}.clf_raw_keys"
  val Node2t_team_k7m_aux_d_tmp_clf_crmcb_filterIN = s"${Stg0Schema}.crm_s_contact"
   val Nodet_team_k7m_aux_d_tmp_clf_crmcb_filterOUT = s"${DevSchema}.tmp_clf_crmcb_filter"
  val dashboardPath = s"${config.auxPath}tmp_clf_crmcb_filter"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_crmcb_filterOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfCrmcbFilter()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_crmcb_filterOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""select distinct t.row_id cont_id
         	from
         	(select distinct
         			trim(regexp_replace(upper(fio),'[^A-ZА-Я]+',' ')) fio
         		from $Node1t_team_k7m_aux_d_tmp_clf_crmcb_filterIN
         		where
         			nvl(trim(regexp_replace(upper(fio),'[^A-ZА-Я]+',' ')),'') <> ''
         		) clf
         		join (
         		select
         			  row_id
         			, trim(regexp_replace(upper(concat(last_name, ' ', fst_name, ' ', mid_name)),'[^A-ZА-Я]+',' ')) fio
         		from  $Node2t_team_k7m_aux_d_tmp_clf_crmcb_filterIN
         		where
         			nvl(trim(regexp_replace(upper(concat(last_name, ' ', fst_name, ' ', mid_name)),'[^A-ZА-Я]+',' ')), '') <> ''
         		) t
         			on t.fio=clf.fio
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_crmcb_filterOUT")

    logInserted()

  }
}

