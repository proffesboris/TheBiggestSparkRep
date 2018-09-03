package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMmdmUdboClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_udboIN = s"${Stg0Schema}.mdm_contractrole"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_udboIN = s"${DevSchema}.tmp_clf_mdm_filter"
  val Node3t_team_k7m_aux_d_tmp_clf_mdm_udboIN = s"${Stg0Schema}.mdm_contractcomponent"
  val Node4t_team_k7m_aux_d_tmp_clf_mdm_udboIN = s"${Stg0Schema}.mdm_cdcontractsttp"
  val Node5t_team_k7m_aux_d_tmp_clf_mdm_udboIN = s"${Stg0Schema}.mdm_contract"
  val  Nodet_team_k7m_aux_d_tmp_clf_mdm_udboOUT = s"${DevSchema}.tmp_clf_mdm_udbo"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_udbo"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_udboOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMmdmUdbo()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_udboOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""		select cont_id,contract_id,contract_st_tp_cd,udbo_status,signed_dt,agreement_name
         	from
         		(select
         			  lnk.cont_id
         			, comp.contract_id
         			, comp.contract_st_tp_cd
         			, tp.name udbo_status
         			--, comp.prod_tp_cd, comp.issue_dt
         			, contract.signed_dt
         			, contract.agreement_name
         			, row_number() over(partition by lnk.cont_id
         			    				order by
         					  case when comp.contract_st_tp_cd = 1 then 1 else 2 end asc -- Сначала действующие договоры
         					, contract.signed_dt desc		-- При наличии нескольких действующих выбираем самый свежий
         				) rn
         		from
         			(select cont_id,contr_component_id from $Node1t_team_k7m_aux_d_tmp_clf_mdm_udboIN where end_dt is null) lnk
         			join $Node2t_team_k7m_aux_d_tmp_clf_mdm_udboIN filter
         				on filter.cont_id = lnk.cont_id
         			join (select contract_id,contract_st_tp_cd,contr_component_id from $Node3t_team_k7m_aux_d_tmp_clf_mdm_udboIN where prod_tp_cd in (4444444,10000000000)) comp
         				on lnk.contr_component_id = comp.contr_component_id
         			join $Node4t_team_k7m_aux_d_tmp_clf_mdm_udboIN tp
         				on tp.contract_st_tp_cd = comp.contract_st_tp_cd and tp.lang_tp_cd = 2200
         			join $Node5t_team_k7m_aux_d_tmp_clf_mdm_udboIN contract
         				on contract.contract_id = comp.contract_id
         			--join internal_mdm_mdm.cdprodtp prod	on prod.prod_tp_cd = comp.prod_tp_cd and prod.lang_tp_cd = 2200
         		where 1 = 1
         		) t
         	where rn = 1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_udboOUT")

    logInserted()

  }
}
