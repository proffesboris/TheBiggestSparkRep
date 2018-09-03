package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmDulClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_dulIN = s"${Stg0Schema}.mdm_identifier"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_dulIN = s"${DevSchema}.tmp_clf_mdm_filter"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_dulOUT = s"${DevSchema}.tmp_clf_mdm_dul"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_dul"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_dulOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmDul()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_dulOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""select cont_id,id_tp_cd,ref_num,ID_SERIES,ID_NUM,sb_issue_dt,expiry_dt,identifier_desc,issue_location
         	from
         		(select
         			  t.cont_id
         			, t.id_tp_cd
         			, t.ref_num 	-- Серия и номер
              , substr(regexp_replace(t.ref_num,'[^0-9]',''),1,4)     as ID_SERIES
              , substr(regexp_replace(t.ref_num,'[^0-9]',''),-6)      as ID_NUM
         			, t.sb_issue_dt
         			, t.expiry_dt
         			, t.identifier_desc	-- Кем выдан
         			, t.issue_location		-- Код подразделения Кем выдан
         			, row_number() over(partition by t.cont_id,	t.id_tp_cd
         			                    order by nvl(t.end_dt,cast('9999-12-31 00:00:00' as timestamp)) desc, t.start_dt desc,t.sb_issue_dt,
         					                         nvl(t.expiry_dt,cast('9999-12-31 00:00:00' as timestamp)) desc) rn
         		from
         			$Node1t_team_k7m_aux_d_tmp_clf_mdm_dulIN t
         			join $Node2t_team_k7m_aux_d_tmp_clf_mdm_dulIN filter
         				on filter.cont_id = t.cont_id
         			--left join internal_mdm_mdm.cdidtp tp on tp.id_tp_cd = t.id_tp_cd
         		where t.id_tp_cd in (21	-- Паспорт РФ
         			,1011	-- ИНН
         			,1013	-- СНИЛС
         			)
         		) t
         	where rn = 1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_dulOUT")

    logInserted()
  }
}