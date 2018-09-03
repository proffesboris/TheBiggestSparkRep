package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmContClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_contIN = s"${Stg0Schema}.mdm_contactmethod"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_contIN = s"${DevSchema}.tmp_clf_mdm_cont_lnk2"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_contOUT = s"${DevSchema}.tmp_clf_mdm_cont"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_cont"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_contOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmCont()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_contOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""	select cont_id, contact_method_id, cont_k7m_type, cont_meth_tp_cd, ref_num
         	from
         		(select
         			  cont_lnk2.cont_id
         			, cont_lnk2.contact_method_id
         			, cont_lnk2.cont_k7m_type
         			, cont_lnk2.cont_meth_tp_cd
         			, t.cont_meth_cat_cd
         			, t.ref_num	-- Номер телефона
         			, row_number() over(partition by cont_lnk2.cont_id, cont_lnk2.cont_k7m_type	order by
         					case
         						when cont_lnk2.cont_k7m_type in ('tel_home', 'tel_mob') and nvl(t.ref_num,'')<>'' then 1 -- проверка заполненности и корректности номера телефона
         						when cont_lnk2.cont_k7m_type in ('email') and t.ref_num like '%@%.%' then 1 -- проверка заполненности и корректности email
         						else 2
         						end asc
         					, cont_lnk2.cont_k7m_priority asc
         				) rn
         	from
         			$Node1t_team_k7m_aux_d_tmp_clf_mdm_contIN t
         			join $Node2t_team_k7m_aux_d_tmp_clf_mdm_contIN cont_lnk2
         				on cont_lnk2.contact_method_id = t.contact_method_id
         	where 1 = 1
         		) t
         	where rn = 1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_contOUT")

    logInserted()

  }
}