package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmContLnk1Class (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_cont_lnk1IN = s"${Stg0Schema}.mdm_locationgroup"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_cont_lnk1IN = s"${DevSchema}.tmp_clf_mdm_filter"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk1OUT = s"${DevSchema}.tmp_clf_mdm_cont_lnk1"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_cont_lnk1"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk1OUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmContLnk1()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk1OUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""	select cont_id,location_group_id,loc_group_tp_code
         	from
         		(select
         			  t.cont_id
         			, t.location_group_id
         			, t.loc_group_tp_code		-- Адрес или Контакт
         			, row_number() over(
         				partition by
         					  t.cont_id
         					, t.location_group_id
         				order by
         					  nvl(t.end_dt, cast('9999-12-31 00:00:00' as timestamp)) desc
         					, t.start_dt desc
         					--, nvl(t.effect_end_tm, cast('9999-12-31 00:00:00' as timestamp)) desc
         					, t.effect_start_tm desc
         				) rn
         		from
         			$Node1t_team_k7m_aux_d_tmp_clf_mdm_cont_lnk1IN t
         			join $Node2t_team_k7m_aux_d_tmp_clf_mdm_cont_lnk1IN filter
         				on filter.cont_id = t.cont_id
         		where 1 = 1
         		) t
         	where rn = 1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk1OUT")

    logInserted()
  }
}