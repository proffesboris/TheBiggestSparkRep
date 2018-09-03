package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmContLnk2Class (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2IN = s"${Stg0Schema}.mdm_contactmethodgroup"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2IN = s"${DevSchema}.tmp_clf_mdm_cont_lnk1"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2OUT = s"${DevSchema}.tmp_clf_mdm_cont_lnk2"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_cont_lnk2"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2OUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmContLnk2()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2OUT).setLevel(Level.WARN)

    val Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2TpRef = Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2OUT.concat("_TpRef")

    val smartSrcHiveTableStage1 = spark.sql(
      s"""		select
         		inline(array(
         			  struct(1, 'tel_home', 1)    -- домашний телефон
         			, struct(17, 'tel_home', 2)    -- домашний телефон доп.
         			, struct(3, 'tel_mob', 1)    -- мобильный телефон
         			, struct(15, 'tel_mob', 2)    -- мобильный телефон доп.
         			, struct(24, 'tel_mob', 3)    -- телефон МБК
         			, struct(29, 'tel_mob', 4)    -- Служебный мобильный номер
         			, struct(4, 'email', 1)    -- персональный email
         			, struct(22, 'email', 2)    -- персональный e-mail доп.
         			, struct(26, 'email', 3)    -- Иное(email)
         		))
         		as (
         			  cont_meth_tp_cd
         			, cont_k7m_type
         			, cont_k7m_priority
         		)
    """
    )
    smartSrcHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_TpRef")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2TpRef")

    val smartSrcHiveTableStage2 = spark.sql(
      s"""			select
         		  cont_lnk1.cont_id
         		, t.contact_method_id
         		, t.cont_meth_tp_cd	--назначение
         		, t.method_st_tp_cd	--статус
         		, cont_tp_ref.cont_k7m_type
         		, cont_tp_ref.cont_k7m_priority
         	from
         		$Node1t_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2IN t
         		join (select cont_id, location_group_id, loc_group_tp_code
         		from $Node2t_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2IN where loc_group_tp_code = 'C') cont_lnk1
         			on cont_lnk1.location_group_id = t.location_group_id
         		--join internal_mdm_mdm.cdcontmethtp tp	on tp.cont_meth_tp_cd = t.cont_meth_tp_cd
         		join $Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2TpRef cont_tp_ref
         			on cont_tp_ref.cont_meth_tp_cd = t.cont_meth_tp_cd
         	where 1 = 1
    """
    )
    smartSrcHiveTableStage2
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_cont_lnk2OUT")

    logInserted()
  }
}
