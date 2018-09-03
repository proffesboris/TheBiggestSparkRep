package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmDedublClass  (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_dedublIN = s"${DevSchema}.clf_mdm"
  val  Nodet_team_k7m_aux_d_tmp_clf_mdm_dedublOUT = s"${DevSchema}.tmp_clf_mdm_dedubl"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_dedubl"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_dedublOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmDedubl()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_dedublOUT).setLevel(Level.WARN)

    val smartSrcHiveTableStage1 = spark.sql(
      s"""	SELECT distinct
          t.mdm_id
         ,t.mdm_active_flag
         ,t.full_name         ,t.clf_l_name         ,t.clf_f_name         ,t.clf_m_name
         ,t.status_active_dead
         ,t.id_series_num
         ,t.id_date          -- 	Дата выдачи
         ,t.identifier_desc  --кем выдан
         ,t.issue_location   --код подразделения
         ,t.ID_SERIES
         ,t.ID_NUM
         ,t.birth_date
         ,t.birth_place
         ,t.id_end_date
         ,t.gender_tp_code
         ,t.citizenship
         ,t.inn
         ,t.adr_reg         ,t.adr_reg_postal_code         ,t.adr_reg_cntry_name         ,t.adr_reg_region         ,t.adr_reg_city_name         ,t.adr_reg_area         ,t.adr_reg_settlement
         ,t.adr_reg_street_name         ,t.adr_reg_house_number         ,t.adr_reg_korpus_number         ,t.adr_reg_building_number         ,t.adr_reg_residence_num
         ,t.adr_reg_kladr         ,t.adr_reg_kladr_1         ,t.adr_reg_kladr_2         ,t.adr_reg_kladr_3         ,t.adr_reg_kladr_4         ,t.adr_reg_kladr_5         ,t.adr_reg_kladr_6
         ,t.adr_fact, t.adr_fact_postal_code, t.adr_fact_cntry_name, t.adr_fact_region, t.adr_fact_city_name, t.adr_fact_area, t.adr_fact_settlement,  t.adr_fact_street_name, t.adr_fact_house_number
         ,t.adr_fact_korpus_number, t.adr_fact_building_number ,t.adr_fact_residence_num
         ,t.adr_fact_kladr         ,t.adr_fact_kladr_1         ,t.adr_fact_kladr_2         ,t.adr_fact_kladr_3         ,t.adr_fact_kladr_4         ,t.adr_fact_kladr_5         ,t.adr_fact_kladr_6
         ,t.tel_home
         ,t.tel_mob
         ,t.email
         ,t.udbo_open_dt         ,t.udbo_active_flag         ,t.udbo_agreement_num
         ,t.snils         ,t.systems_count         ,t.last_update_dt
         ,t.full_name_clear    ,t.id_series_num_clear      ,t.inn_clear      ,t.birth_date_clear         ,t.tel_mob_clear --Поля для ключевания
         -- Ключ 1: ФИО + ДР + ДУЛ
         	, cast((case when (full_name_clear is not null) and (id_series_num_clear is not null) and (birth_date_clear is not null) then
         	  row_number() over(partition by full_name_clear, birth_date_clear, id_series_num_clear
         		order by
         			  case when mdm_active_flag = 'Y' then 1 else 2 end		-- сначала записи в активном статусе
         			, case when udbo_active_flag = 'Y' then 1 else 2 end	-- сначала записи с действующим договором
              , systems_count desc
         			, last_update_dt desc
              , id_date
         		) end) as string) as rn_id
         -- Ключ 2: ФИО + др + телефон
         	, cast((case when (full_name_clear is not null) and (birth_date_clear is not null) and (tel_mob_clear is not null) then
         	  row_number() over(partition by full_name_clear, birth_date_clear, tel_mob_clear
         		order by
         			  case when mdm_active_flag = 'Y' then 1 else 2 end		-- сначала записи в активном статусе
         			, case when udbo_active_flag = 'Y' then 1 else 2 end	-- сначала записи с действующим договором
         			, systems_count desc
          		, last_update_dt desc
         		) end) as string) as rn_tel
         -- Ключ 3: ФИО + ИНН
         ,cast(null as string) as rn_inn
         -- Ключ 4: ИНН
         ,cast(null as string) as rn_inn_only
         from	$Node1t_team_k7m_aux_d_tmp_clf_mdm_dedublIN t
         where inn_clear is null
    """
    )
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_dedublOUT")

        val smartSrcHiveTableStage2 = spark.sql(
      s"""	SELECT distinct
       t.mdm_id
      ,t.mdm_active_flag
      ,t.full_name         ,t.clf_l_name         ,t.clf_f_name         ,t.clf_m_name
      ,t.status_active_dead
      ,t.id_series_num         ,t.id_date         ,t.identifier_desc
      ,t.issue_location
       ,t.ID_SERIES
       ,t.ID_NUM
      ,t.birth_date
      ,t.birth_place
      ,t.id_end_date
      ,t.gender_tp_code
      ,t.citizenship
      ,t.inn
      ,t.adr_reg         ,t.adr_reg_postal_code         ,t.adr_reg_cntry_name         ,t.adr_reg_region         ,t.adr_reg_city_name         ,t.adr_reg_area         ,t.adr_reg_settlement
      ,t.adr_reg_street_name         ,t.adr_reg_house_number         ,t.adr_reg_korpus_number         ,t.adr_reg_building_number         ,t.adr_reg_residence_num
      ,t.adr_reg_kladr         ,t.adr_reg_kladr_1         ,t.adr_reg_kladr_2         ,t.adr_reg_kladr_3         ,t.adr_reg_kladr_4         ,t.adr_reg_kladr_5         ,t.adr_reg_kladr_6
      ,t.adr_fact, t.adr_fact_postal_code, t.adr_fact_cntry_name, t.adr_fact_region, t.adr_fact_city_name, t.adr_fact_area, t.adr_fact_settlement,  t.adr_fact_street_name, t.adr_fact_house_number
      ,t.adr_fact_korpus_number, t.adr_fact_building_number ,t.adr_fact_residence_num
      ,t.adr_fact_kladr         ,t.adr_fact_kladr_1         ,t.adr_fact_kladr_2         ,t.adr_fact_kladr_3         ,t.adr_fact_kladr_4         ,t.adr_fact_kladr_5         ,t.adr_fact_kladr_6
      ,t.tel_home
      ,t.tel_mob
      ,t.email
      ,t.udbo_open_dt         ,t.udbo_active_flag         ,t.udbo_agreement_num
      ,t.snils         ,t.systems_count         ,t.last_update_dt
      ,t.full_name_clear    ,t.id_series_num_clear      ,t.inn_clear      ,t.birth_date_clear         ,t.tel_mob_clear --Поля для ключевания
      -- Ключ 1: ФИО + ДР + ДУЛ
    ,cast(null as string) rn_id
    -- Ключ 2: ФИО + др + телефон
    ,cast(null as string) rn_tel
    -- Ключ 3: ФИО + ИНН
        ,cast((case when (full_name_clear is not null) and (inn_clear is not null) then
         row_number() over( partition by  full_name_clear, inn_clear
       order by
       	  case when mdm_active_flag = 'Y' then 1 else 2 end		-- сначала записи в активном статусе
       	, case when udbo_active_flag = 'Y' then 1 else 2 end	-- сначала записи с действующим договором
       	, systems_count desc
       	, last_update_dt desc
       ) end) as string) as rn_inn
    -- Ключ 4: ИНН
         	, cast((case when (inn_clear is not null) then  row_number() over( partition by inn_clear
         		order by
         			  case when mdm_active_flag = 'Y' then 1 else 2 end		-- сначала записи в активном статусе
         			, case when udbo_active_flag = 'Y' then 1 else 2 end	-- сначала записи с действующим договором
         			, systems_count desc
         			, last_update_dt desc
         		) end) as string) as rn_inn_only
         from	$Node1t_team_k7m_aux_d_tmp_clf_mdm_dedublIN t
         where inn_clear is not null and  birth_date_clear is null
"""
    )
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_dedublOUT")

    val smartSrcHiveTableStage3 = spark.sql(
      s"""	SELECT distinct
       t.mdm_id
      ,t.mdm_active_flag
      ,t.full_name         ,t.clf_l_name         ,t.clf_f_name         ,t.clf_m_name
      ,t.status_active_dead
      ,t.id_series_num         ,t.id_date         ,t.identifier_desc
      ,t.issue_location
      ,t.ID_SERIES
      ,t.ID_NUM
      ,t.birth_date
      ,t.birth_place
      ,t.id_end_date
      ,t.gender_tp_code
      ,t.citizenship
      ,t.inn
      ,t.adr_reg         ,t.adr_reg_postal_code         ,t.adr_reg_cntry_name         ,t.adr_reg_region         ,t.adr_reg_city_name         ,t.adr_reg_area         ,t.adr_reg_settlement
      ,t.adr_reg_street_name         ,t.adr_reg_house_number         ,t.adr_reg_korpus_number         ,t.adr_reg_building_number         ,t.adr_reg_residence_num
      ,t.adr_reg_kladr         ,t.adr_reg_kladr_1         ,t.adr_reg_kladr_2         ,t.adr_reg_kladr_3         ,t.adr_reg_kladr_4         ,t.adr_reg_kladr_5         ,t.adr_reg_kladr_6
      ,t.adr_fact, t.adr_fact_postal_code, t.adr_fact_cntry_name, t.adr_fact_region, t.adr_fact_city_name, t.adr_fact_area, t.adr_fact_settlement,  t.adr_fact_street_name, t.adr_fact_house_number
      ,t.adr_fact_korpus_number, t.adr_fact_building_number ,t.adr_fact_residence_num
      ,t.adr_fact_kladr         ,t.adr_fact_kladr_1         ,t.adr_fact_kladr_2         ,t.adr_fact_kladr_3         ,t.adr_fact_kladr_4         ,t.adr_fact_kladr_5         ,t.adr_fact_kladr_6
      ,t.tel_home
      ,t.tel_mob
      ,t.email
      ,t.udbo_open_dt         ,t.udbo_active_flag         ,t.udbo_agreement_num
      ,t.snils         ,t.systems_count         ,t.last_update_dt
      ,t.full_name_clear    ,t.id_series_num_clear      ,t.inn_clear      ,t.birth_date_clear         ,t.tel_mob_clear --Поля для ключевания
    -- Ключ 1: ФИО + ДР + ДУЛ
    	, cast((case when (full_name_clear is not null) and (id_series_num_clear is not null) and (birth_date_clear is not null) then
    	  row_number() over(partition by full_name_clear, birth_date_clear, id_series_num_clear
    		order by
    			  case when mdm_active_flag = 'Y' then 1 else 2 end		-- сначала записи в активном статусе
    			, case when udbo_active_flag = 'Y' then 1 else 2 end	-- сначала записи с действующим договором
    			, systems_count desc
    			, last_update_dt desc
          , id_date
    		) end) as string) as rn_id
    -- Ключ 2: ФИО + др + телефон
    	, cast((case when (full_name_clear is not null) and (birth_date_clear is not null) and (tel_mob_clear is not null) then
    	  row_number() over(partition by full_name_clear, birth_date_clear, tel_mob_clear
    		order by
    			  case when mdm_active_flag = 'Y' then 1 else 2 end		-- сначала записи в активном статусе
    			, case when udbo_active_flag = 'Y' then 1 else 2 end	-- сначала записи с действующим договором
    			, systems_count desc
     		, last_update_dt desc
    		) end) as string) as rn_tel
    -- Ключ 3: ФИО + ИНН
        ,cast((case when (full_name_clear is not null) and (inn_clear is not null) then
         row_number() over( partition by  full_name_clear, inn_clear
       order by
       	  case when mdm_active_flag = 'Y' then 1 else 2 end		-- сначала записи в активном статусе
       	, case when udbo_active_flag = 'Y' then 1 else 2 end	-- сначала записи с действующим договором
       	, systems_count desc
       	, last_update_dt desc
       ) end) as string) as rn_inn
    -- Ключ 4: ИНН
         	,cast((case when (inn_clear is not null) then  row_number() over( partition by inn_clear
         		order by
         			  case when mdm_active_flag = 'Y' then 1 else 2 end		-- сначала записи в активном статусе
         			, case when udbo_active_flag = 'Y' then 1 else 2 end	-- сначала записи с действующим договором
         			, systems_count desc
         			, last_update_dt desc
         		) end) as string) as rn_inn_only
         from	$Node1t_team_k7m_aux_d_tmp_clf_mdm_dedublIN t
         where inn_clear is not null and  birth_date_clear is not null
"""
    )
    smartSrcHiveTableStage3
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_dedublOUT")

    logInserted()

  }
}
