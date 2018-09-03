package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CLFMdmClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

   val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_mdmIN = s"${DevSchema}.tmp_clf_mdm_clf"
  val Node2t_team_k7m_aux_d_clf_mdmIN = s"${DevSchema}.tmp_clf_mdm_clf_status"
  val Node3t_team_k7m_aux_d_clf_mdmIN = s"${DevSchema}.tmp_clf_mdm_clf_name"
  val Node4t_team_k7m_aux_d_clf_mdmIN = s"${DevSchema}.tmp_clf_mdm_pers"
  val Node5t_team_k7m_aux_d_clf_mdmIN = s"${DevSchema}.tmp_clf_mdm_cntry"
  val Node6t_team_k7m_aux_d_clf_mdmIN = s"${DevSchema}.tmp_clf_mdm_dul"
  val Node7t_team_k7m_aux_d_clf_mdmIN = s"${DevSchema}.tmp_clf_mdm_cont"
  val Node8t_team_k7m_aux_d_clf_mdmIN = s"${DevSchema}.tmp_clf_mdm_adr"
  val Node9t_team_k7m_aux_d_clf_mdmIN = s"${DevSchema}.tmp_clf_mdm_udbo"
  val Node10t_team_k7m_aux_d_clf_mdmIN = s"${DevSchema}.tmp_clf_mdm_systems"
  val  Nodet_team_k7m_aux_d_clf_mdmOUT = s"${DevSchema}.clf_mdm"
  val dashboardPath = s"${config.auxPath}clf_mdm"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_mdmOUT //витрина
  override def processName: String = "CLF"

  def DoCLFMdm()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_mdmOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""	SELECT distinct
         	  clf.cont_id mdm_id 					-- a.	Id клиента фл в МДМ (CLF.MDM_ID)
         	, case
         		when clf.inactivated_dt is null then 'Y'
         		else 'N'
         		end mdm_active_flag 				-- b.	Флаг активности записи в МДМ (Y/N)
         	--, clf_status.name						-- наименование статуса в МДМ
         	, clf.contact_name full_name
         	, clf_name.clf_l_name
         	, clf_name.clf_f_name
         	, clf_name.clf_m_name
         	, case
         		when pers.deceased_dt is not null or clf.client_st_tp_cd = 0 then 'DEAD'
         		else 'ACTIVE'
         		end status_active_dead				-- c.	Статус клиента (ACTIVE/DEAD)
         	, pasp_rf.ref_num id_series_num			-- d.	ДУЛ (Серия и номер) (если поиск фл осуществлялся по ключу ФИО+ДР+Мобильный телефон или ФИО+ИННфл)
         	, pasp_rf.sb_issue_dt id_date			-- 		Дата выдачи
         	, pasp_rf.identifier_desc				--		Кем выдан
         	, pasp_rf.issue_location				-- 		Код подразделения
          ,pasp_rf.ID_SERIES
          ,pasp_rf.ID_NUM
         	, pers.birth_dt birth_date				-- e.	Дата рождения (если поиск фл осуществлялся по ключу ФИО+ИННфл)
          , pers.sb_birth_place  as birth_place   -- Место рождения
         	--,pty.death_dt
         	--,job.job_title_name					-- f.	Должность
         	, pasp_rf.expiry_dt id_end_date			-- g.	Дата истечения ДУЛ
         	, pers.gender_tp_code					-- h.	Пол
         	, cntry.name citizenship				-- j.	Гражданство
         	, inn.ref_num inn						-- k.	ИНН фл
         	, adr_reg.adr_full 			adr_reg		-- l.	Адрес регистрации (список полей),
         	, adr_reg.postal_code		adr_reg_postal_code
         	, adr_reg.cntry_name		adr_reg_cntry_name
          , adr_reg.region			adr_reg_region
         	, adr_reg.city_name			adr_reg_city_name
         	, adr_reg.sb_area			adr_reg_area
         	, adr_reg.sb_settlement		adr_reg_settlement
         	, adr_reg.street_name		adr_reg_street_name
          , adr_reg.street_number		adr_reg_house_number
         	, adr_reg.box_id			adr_reg_korpus_number
         	, adr_reg.building_name		adr_reg_building_number
         	, adr_reg.residence_num		adr_reg_residence_num
         	, adr_reg.sb_kladr			adr_reg_kladr
         	, adr_reg.sb_kladr_1_type	adr_reg_kladr_1
         	, adr_reg.sb_kladr_2_type	adr_reg_kladr_2
         	, adr_reg.sb_kladr_3_type	adr_reg_kladr_3
         	, adr_reg.sb_kladr_4_type	adr_reg_kladr_4
         	, adr_reg.sb_kladr_5_type	adr_reg_kladr_5
         	, adr_reg.sb_kladr_6_type	adr_reg_kladr_6
         	, adr_fact.adr_full 		adr_fact	-- m.	Адрес фактического проживания (список полей),
         	, adr_fact.postal_code		adr_fact_postal_code
         	, adr_fact.cntry_name		adr_fact_cntry_name
         	, adr_fact.region			adr_fact_region
         	, adr_fact.city_name		adr_fact_city_name
         	, adr_fact.sb_area			adr_fact_area
         	, adr_fact.sb_settlement	adr_fact_settlement
         	, adr_fact.street_name		adr_fact_street_name
         	, adr_fact.street_number	adr_fact_house_number
         	, adr_fact.box_id			adr_fact_korpus_number
         	, adr_fact.building_name	adr_fact_building_number
         	, adr_fact.residence_num	adr_fact_residence_num
         	, adr_fact.sb_kladr			adr_fact_kladr
         	, adr_fact.sb_kladr_1_type	adr_fact_kladr_1
         	, adr_fact.sb_kladr_2_type	adr_fact_kladr_2
         	, adr_fact.sb_kladr_3_type	adr_fact_kladr_3
         	, adr_fact.sb_kladr_4_type	adr_fact_kladr_4
         	, adr_fact.sb_kladr_5_type	adr_fact_kladr_5
         	, adr_fact.sb_kladr_6_type	adr_fact_kladr_6
         	, tel_home.ref_num tel_home				-- n.	Телефон (домашний),
         	, tel_mob.ref_num tel_mob				-- o.	Телефон (мобильный),
         	, email.ref_num email					-- p.	Адрес электронной почты,
         	, udbo.signed_dt udbo_open_dt			-- q.	Дата открытия договора УДБО/ДБО,
         	, case
         		when udbo.contract_st_tp_cd = 1 then 'Y'
         		else 'N'
            end udbo_active_flag				-- r.	Признак действия договора УДБО/ДБО,
         	--,udbo.udbo_status						--		Статус договора
         	, udbo.agreement_name udbo_agreement_num -- s.	Номер договора УДБО/ДБО.
         	, snils.ref_num snils					-- t.	СНИЛС
         	, systems.systems_count					-- Количество ссылок с "золотого" клиента МДМ на клиентов АС.
         	, clf.last_update_dt
         	-- Дальше идут атрибуты, по которым делается матчинг. Для целей матчинга в этих атрибутах производится очистка.
         	, case
         		when nvl(regexp_replace(upper(concat(clf_name.clf_l_name, clf_name.clf_f_name, clf_name.clf_m_name)), '[^A-ZА-Я]+',''), '') = ''
         			then case
         				when nvl(regexp_replace(upper(clf.contact_name), '[^A-ZА-Я]+',''), '') = ''
         					then null
         				else trim(regexp_replace(upper(clf.contact_name), '[^A-ZА-Я]+',' '))
         				end
         		else trim(regexp_replace(upper(concat(clf_name.clf_l_name, ' ', clf_name.clf_f_name, ' ', clf_name.clf_m_name)),'[^A-ZА-Я]+',' '))
         		end full_name_clear															-- Полное ФИО для матчинга:
         																					-- В верхнем регистре
         																					-- Только буквы и пробелы (дефисы и прочие знаки заменяются на пробелы)
         																					-- Одиночные пробелы между словами, без пробелов в начале и конце строки
         	, case
         		when length(regexp_replace(pasp_rf.ref_num,'[^0-9]','')) = 10
         		then regexp_replace(pasp_rf.ref_num,'[^0-9]','')
         		else null
         		end id_series_num_clear														-- Серия и номер паспорта для матчинга: только цифры, все пробелы удалены, серия и номер идут подряд без пробела.
         	, case
            when (length(regexp_replace(inn.ref_num,'[^0-9]','')) = 12) and (regexp_replace(inn.ref_num,'[^0-9]','') not in ('111111111111','100000000000','000000000000'))
            then regexp_replace(inn.ref_num,'[^0-9]','')
            else null
            end inn_clear																-- ИНН для матчинга: только цифры, все пробелы удалены.
         	, case
         		when nvl(date_format(pers.birth_dt, 'yyyy-MM-dd'),'') <> ''
         		then date_format(pers.birth_dt, 'yyyy-MM-dd')
         		else null
         		end birth_date_clear														-- Дата рождения в формате строки '2018-01-01'
         	, case
         		when nvl(regexp_replace(tel_mob.ref_num,'[^0-9]',''),'') <> ''
         		then regexp_replace(tel_mob.ref_num,'[^0-9]','')
         		else null
         		end tel_mob_clear															-- Телефон (мобильный)
 FROM    	$Node1t_team_k7m_aux_d_clf_mdmIN clf
         	join $Node2t_team_k7m_aux_d_clf_mdmIN clf_status
         		on clf_status.client_st_tp_cd = clf.client_st_tp_cd
         	join $Node3t_team_k7m_aux_d_clf_mdmIN clf_name
         		on clf_name.cont_id = clf.cont_id
         	join $Node4t_team_k7m_aux_d_clf_mdmIN pers
         		on pers.cont_id = clf.cont_id
         	left join $Node5t_team_k7m_aux_d_clf_mdmIN cntry
         		on cntry.country_tp_cd = pers.citizenship_tp_cd
         	left join (select cont_id,ref_num,sb_issue_dt,identifier_desc,issue_location,expiry_dt,ID_SERIES,ID_NUM	from $Node6t_team_k7m_aux_d_clf_mdmIN where id_tp_cd = 21) pasp_rf
         		on pasp_rf.cont_id = clf.cont_id
         	left join (select cont_id,ref_num	from $Node6t_team_k7m_aux_d_clf_mdmIN where id_tp_cd = 1011) inn
         		on inn.cont_id = clf.cont_id
         	left join (select cont_id,ref_num	from $Node6t_team_k7m_aux_d_clf_mdmIN where id_tp_cd = 1013) snils
         		on snils.cont_id = clf.cont_id
         	left join (select cont_id,ref_num	from $Node7t_team_k7m_aux_d_clf_mdmIN where cont_k7m_type = 'tel_home') tel_home
         		on tel_home.cont_id = clf.cont_id
         	left join (select cont_id,ref_num	from $Node7t_team_k7m_aux_d_clf_mdmIN where cont_k7m_type = 'tel_mob') tel_mob
         		on tel_mob.cont_id = clf.cont_id
         	left join (select cont_id,ref_num	from $Node7t_team_k7m_aux_d_clf_mdmIN where cont_k7m_type = 'email') email
         		on email.cont_id = clf.cont_id
         	left join (select cont_id,adr_full,postal_code,cntry_name,region,city_name,sb_area,sb_settlement,street_name,street_number,box_id,building_name,residence_num,sb_kladr,
         				sb_kladr_1_type,sb_kladr_2_type,sb_kladr_3_type,sb_kladr_4_type,sb_kladr_5_type,sb_kladr_6_type	from $Node8t_team_k7m_aux_d_clf_mdmIN where adr_k7m_type = 'adr_reg') adr_reg
         		on adr_reg.cont_id = clf.cont_id
         	left join (select cont_id,adr_full,postal_code,cntry_name,region,city_name,sb_area,sb_settlement,street_name,street_number,box_id,building_name,
         residence_num, sb_kladr, sb_kladr_1_type, sb_kladr_2_type, sb_kladr_3_type, sb_kladr_4_type, sb_kladr_5_type, sb_kladr_6_type
         from $Node8t_team_k7m_aux_d_clf_mdmIN where adr_k7m_type = 'adr_fact') adr_fact
         		on adr_fact.cont_id = clf.cont_id
         	left join $Node9t_team_k7m_aux_d_clf_mdmIN udbo
         		on udbo.cont_id = clf.cont_id
         	left join (select cont_id, count(1) systems_count from $Node10t_team_k7m_aux_d_clf_mdmIN group by cont_id) systems
         		on systems.cont_id = clf.cont_id
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_mdmOUT")

    logInserted()

  }
}