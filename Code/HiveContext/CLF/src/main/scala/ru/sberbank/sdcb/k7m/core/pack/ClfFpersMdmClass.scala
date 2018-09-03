package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfFpersMdmClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_fpers_mdmIN = s"${DevSchema}.clf_fpers_mdm_ki"
  val  Nodet_team_k7m_aux_d_clf_fpers_mdm_tOUT = s"${DevSchema}.clf_fpers_mdm_t"
  val  Nodet_team_k7m_aux_d_clf_fpers_mdmOUT = s"${DevSchema}.clf_fpers_mdm"
//---Витрина MDM для смэчившихся(хотябы по 1 ключу) физиков из связей (clf_raw_keys)
  val dashboardPathT = s"${config.auxPath}clf_fpers_mdm_t"
  val dashboardPath = s"${config.auxPath}clf_fpers_mdm"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_fpers_mdmOUT //витрина
  override def processName: String = "CLF"

  def DoClfFpersMdm()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_fpers_mdmOUT).setLevel(Level.WARN)

    val smartSrcHiveTableStage1 = spark.sql(
      s""" select distinct
          id
         ,position_eks
         ,position_ul
         ,doc_ser_eks
         ,doc_num_eks
         ,doc_date_eks
         ,crm_id
         ,inn_eks
         ,inn_ul
         ,inn_ex
         ,inn_gs
         ,mdm_id
         ,mdm_active_flag
         ,full_name
         ,clf_l_name
         ,clf_f_name
         ,clf_m_name
         ,status_active_dead
         ,id_series_num
         ,id_date
         ,identifier_desc
         ,issue_location
         ,ID_SERIES
         ,ID_NUM
         ,birth_date
         ,birth_place
         ,id_end_date
         ,gender_tp_code
         ,citizenship
         ,inn
         ,adr_reg
         ,adr_reg_postal_code
         ,adr_reg_cntry_name
         ,adr_reg_region
         ,adr_reg_city_name
         ,adr_reg_area
         ,adr_reg_settlement
         ,adr_reg_street_name
         ,adr_reg_house_number
         ,adr_reg_korpus_number
         ,adr_reg_building_number
         ,adr_reg_residence_num
         ,adr_reg_kladr
         ,adr_reg_kladr_1
         ,adr_reg_kladr_2
         ,adr_reg_kladr_3
         ,adr_reg_kladr_4
         ,adr_reg_kladr_5
         ,adr_reg_kladr_6
         ,adr_fact
         ,adr_fact_postal_code
         ,adr_fact_cntry_name
         ,adr_fact_region
         ,adr_fact_city_name
         ,adr_fact_area
         ,adr_fact_settlement
         ,adr_fact_street_name
         ,adr_fact_house_number
         ,adr_fact_korpus_number
         ,adr_fact_building_number
         ,adr_fact_residence_num
         ,adr_fact_kladr
         ,adr_fact_kladr_1
         ,adr_fact_kladr_2
         ,adr_fact_kladr_3
         ,adr_fact_kladr_4
         ,adr_fact_kladr_5
         ,adr_fact_kladr_6
         ,tel_home
         ,tel_mob
         ,email
         ,udbo_open_dt
         ,udbo_active_flag
         ,udbo_agreement_num
         ,snils
         ,systems_count
         ,last_update_dt
         ,full_name_clear
         ,id_series_num_clear
         ,inn_clear
         ,birth_date_clear
         ,tel_mob_clear
         ,rn_inn
         ,rn_id
         ,rn_tel
         ,rn_inn_only
         ,k1
         ,k2
         ,k3
         ,k4
         ,m1
         ,m2
         ,m3
         ,m4
         ,mkj
         from $Node1t_team_k7m_aux_d_clf_fpers_mdmIN where mkj <> -1
             """
    )
    smartSrcHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPathT).saveAsTable(s"$Nodet_team_k7m_aux_d_clf_fpers_mdm_tOUT")

//---Витрина MDM 1 mdm_id на связь
    val smartSrcHiveTableStage2 = spark.sql(
      s""" select * from
           (select distinct
          id
         ,position_eks
         ,position_ul
         ,doc_ser_eks
         ,doc_num_eks
         ,doc_date_eks
         ,crm_id
         ,inn_eks
         ,inn_ul
         ,inn_ex
         ,inn_gs
         ,mdm_id
         ,mdm_active_flag
         ,full_name
         ,clf_l_name
         ,clf_f_name
         ,clf_m_name
         ,status_active_dead
         ,id_series_num
         ,id_date
         ,identifier_desc
         ,issue_location
         ,ID_SERIES
         ,ID_NUM
         ,birth_date
         ,birth_place
         ,id_end_date
         ,gender_tp_code
         ,citizenship
         ,inn
         ,adr_reg
         ,adr_reg_postal_code
         ,adr_reg_cntry_name
         ,adr_reg_region
         ,adr_reg_city_name
         ,adr_reg_area
         ,adr_reg_settlement
         ,adr_reg_street_name
         ,adr_reg_house_number
         ,adr_reg_korpus_number
         ,adr_reg_building_number
         ,adr_reg_residence_num
         ,adr_reg_kladr
         ,adr_reg_kladr_1
         ,adr_reg_kladr_2
         ,adr_reg_kladr_3
         ,adr_reg_kladr_4
         ,adr_reg_kladr_5
         ,adr_reg_kladr_6
         ,adr_fact
         ,adr_fact_postal_code
         ,adr_fact_cntry_name
         ,adr_fact_region
         ,adr_fact_city_name
         ,adr_fact_area
         ,adr_fact_settlement
         ,adr_fact_street_name
         ,adr_fact_house_number
         ,adr_fact_korpus_number
         ,adr_fact_building_number
         ,adr_fact_residence_num
         ,adr_fact_kladr
         ,adr_fact_kladr_1
         ,adr_fact_kladr_2
         ,adr_fact_kladr_3
         ,adr_fact_kladr_4
         ,adr_fact_kladr_5
         ,adr_fact_kladr_6
         ,tel_home
         ,tel_mob
         ,email
         ,udbo_open_dt
         ,udbo_active_flag
         ,udbo_agreement_num
         ,snils
         ,systems_count
         ,last_update_dt
         ,full_name_clear
         ,id_series_num_clear
         ,inn_clear
         ,birth_date_clear
         ,tel_mob_clear
         ,rn_inn
         ,rn_id
         ,rn_tel
         ,rn_inn_only
         ,k1
         ,k2
         ,k3
         ,k4
         ,m1
         ,m2
         ,m3
         ,m4
         ,mkj
         ,row_number() over(
         		partition by m.id
                     order by
                       mkj
          			, case when mdm_active_flag = 'Y' then 1 else 2 end		-- сначала записи в активном статусе
         			, case when udbo_active_flag = 'Y' then 1 else 2 end	-- сначала записи с действующим договором
         			, systems_count desc
         			, last_update_dt desc
              , id_date desc
         			, mdm_id desc---на крайняк
         			) as rn
         from $Nodet_team_k7m_aux_d_clf_fpers_mdm_tOUT m) t
         where t.rn = 1
             """
    )
    smartSrcHiveTableStage2
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_fpers_mdmOUT")

    logInserted()

  }

}