package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmAdrLnkClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_adr_lnkIN = s"${Stg0Schema}.mdm_addressgroup"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_adr_lnkIN = s"${DevSchema}.tmp_clf_mdm_cont_lnk1"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_adr_lnkOUT = s"${DevSchema}.tmp_clf_mdm_adr_lnk"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_adr_lnk"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_adr_lnkOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmAdrLnk()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_adr_lnkOUT).setLevel(Level.WARN)

    val Nodet_team_k7m_aux_d_tmp_clf_mdm_adr_lnkTpAdr = Nodet_team_k7m_aux_d_tmp_clf_mdm_adr_lnkOUT.concat("_TpAdr")

    val smartSrcHiveTableStage1 = spark.sql(
      s"""	select
         		inline(array(
         			  struct(1, 'adr_reg', 1)    --адрес регистрации
         			, struct(14, 'adr_reg', 2)    --адрес регистрации пребыв.
         			, struct(18, 'adr_reg', 3)    --адрес регистрации транслитерированный
         			, struct(17, 'adr_reg', 4)    --адрес регистрации пребыв. транслитерированный
         			, struct(6, 'adr_reg', 5)    --Юридический
         			, struct(10, 'adr_reg', 6)    --Транслитерированный юридический адрес
         			, struct(2, 'adr_fact', 1)    --адрес проживания
         			, struct(13, 'adr_fact', 2)    --адрес проживания доп.
         			, struct(7, 'adr_fact', 3)    --Фактический
         			, struct(5, 'adr_fact', 4)    --Почтовый
         			, struct(3, 'adr_fact', 5)    --адрес для получения пенсии военными пенсионерами
         			, struct(4, 'adr_fact', 6)    --адрес для почтовых уведомлений/для получения отчета по счету
         			, struct(15, 'adr_fact', 7)    --адрес проживания транслитерированный
         			, struct(16, 'adr_fact', 8)    --адрес проживания доп. транслитерированный
         			, struct(11, 'adr_fact', 9)    --Транслитерированный фактический адрес
         			, struct(12, 'adr_fact', 10)    --Транслитерированный почтовый адрес
         		))
         		as (
         			addr_usage_tp_cd
         			, adr_k7m_type
         			, adr_k7m_priority
         		)
    """
    )
    smartSrcHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_TpAdr"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_adr_lnkTpAdr")

    val smartSrcHiveTableStage2 = spark.sql(
      s""" select
         		  cont_lnk1.cont_id
         		, t.address_id
         		, t.addr_usage_tp_cd	--назначение
         		, t.sb_addr_st_tp_cd	--статус
         		, adr_tp_ref.adr_k7m_type
         		, adr_tp_ref.adr_k7m_priority
         	from
         		$Node1t_team_k7m_aux_d_tmp_clf_mdm_adr_lnkIN t
         		join (select cont_id, location_group_id, loc_group_tp_code from $Node2t_team_k7m_aux_d_tmp_clf_mdm_adr_lnkIN where loc_group_tp_code = 'A') cont_lnk1
         			on cont_lnk1.location_group_id = t.location_group_id
         		--join internal_mdm_mdm.cdcontmethtp tp on tp.addr_usage_tp_cd = t.addr_usage_tp_cd
         		join $Nodet_team_k7m_aux_d_tmp_clf_mdm_adr_lnkTpAdr adr_tp_ref
         			on adr_tp_ref.addr_usage_tp_cd = t.addr_usage_tp_cd
         	where 1 = 1
    """
    )
    smartSrcHiveTableStage2
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_adr_lnkOUT")

    logInserted()

  }
}
