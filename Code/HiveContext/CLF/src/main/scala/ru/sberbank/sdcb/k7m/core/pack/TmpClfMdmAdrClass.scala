package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmAdrClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_adrIN = s"${Stg0Schema}.mdm_address"
  val Node2t_team_k7m_aux_d_tmp_clf_mdm_adrIN = s"${DevSchema}.tmp_clf_mdm_adr_lnk"
  val Node3t_team_k7m_aux_d_tmp_clf_mdm_adrIN = s"${DevSchema}.tmp_clf_mdm_cntry"
  val  Nodet_team_k7m_aux_d_tmp_clf_mdm_adrOUT = s"${DevSchema}.tmp_clf_mdm_adr"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_adr"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_adrOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmAdr()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_adrOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""	select cont_id, adr_k7m_type, addr_usage_tp_cd, address_id, adr_full, postal_code, cntry_name,region, city_name, sb_area, sb_settlement, street_name, street_number, box_id, building_name, residence_num,
         sb_kladr, sb_kladr_1_type, sb_kladr_2_type, sb_kladr_3_type, sb_kladr_4_type, sb_kladr_5_type, sb_kladr_6_type
         	from
         		(select
         		  adr_lnk.cont_id
         		, adr_lnk.adr_k7m_type
         			, adr_lnk.addr_usage_tp_cd
         			, t.address_id
         			, nvl(t.addr_line_one, t.addr_line_two) adr_full
         			, t.postal_code
         			, cntry.name cntry_name
         			, t.region
         			, t.city_name
         			, t.sb_area
         			, t.sb_settlement
         			, t.street_name
         			, t.street_number
         			, t.box_id
         			, t.building_name
         			, t.residence_num
         			, t.sb_kladr
         			, t.sb_kladr_1_type
         			, t.sb_kladr_2_type
         			, t.sb_kladr_3_type
         			, t.sb_kladr_4_type
         			, t.sb_kladr_5_type
         			, t.sb_kladr_6_type
         			, row_number() over(partition by adr_lnk.cont_id, adr_lnk.adr_k7m_type
         				order by
         					--case when t.addr_standard_ind = 'Y' then 1 else 2 end asc	-- флаг соответствия адреса стандарту
         					  case when nvl(t.sb_kladr_6_type,'') <> '' then 1 else 2 end asc
         					, case when nvl(t.sb_kladr,'') <> '' then 1 else 2 end asc
         					, case when nvl(t.postal_code,'') <> '' then 1 else 2 end asc
         					, adr_lnk.adr_k7m_priority asc
         					, length(t.addr_line_one) desc
         				) rn
         		from
         			$Node1t_team_k7m_aux_d_tmp_clf_mdm_adrIN t
         			join $Node2t_team_k7m_aux_d_tmp_clf_mdm_adrIN adr_lnk
         				on adr_lnk.address_id = t.address_id
         			left join $Node3t_team_k7m_aux_d_tmp_clf_mdm_adrIN cntry
         				on cntry.country_tp_cd = t.country_tp_cd
         		where 1 = 1
         		) t
         	where rn = 1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_adrOUT")

    logInserted()

  }
}