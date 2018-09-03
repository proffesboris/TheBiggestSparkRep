package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisClientClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val DevSchema = config.aux
  val Stg0Schema = config.stg

  val Node1t_team_k7m_aux_d_basis_clientIN = s"${DevSchema}.basis_client0"
  val Node2t_team_k7m_aux_d_basis_clientIN = s"${Stg0Schema}.crm_green_list"
  val Nodet_team_k7m_aux_d_basis_clientOUT = s"${DevSchema}.basis_client"
  val dashboardPath = s"${config.auxPath}basis_client"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_clientOUT //витрина
  override def processName: String = "Basis"

  def DoBasisClient(limit: Int)//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_clientOUT).setLevel(Level.WARN)
    logStart()

    val limitString: String = if (limit > 0) {s"        left semi join $Node2t_team_k7m_aux_d_basis_clientIN l on l.org_crm_id = c.org_crm_id"} else {""}

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
select
                  cast(c.org_crm_id as string) as org_crm_id,
        					c.org_short_crm_name,
        					c.org_crm_name,
        					c.org_ogrn_egrul_num,
        					c.org_inn_crm_num,
        					c.org_kpp_crm_num,
        					c.org_ogrn_crm_num,
        					c.indust_crm_name,
        					c.indust_crm_sic_cd,
        					c.okved_egrul_name,
        					c.isresident_crm_cd,
        					c.tb_crm_name,
        					c.tb_vko_crm_id,
        					c.tb_vko_crm_name,
        					cast(c.org_reg_crm_dt as timestamp) as org_reg_crm_dt,     								--@@@NEW_3 Постнова: тип timestamp
        					cast(c.org_reg_pravo_dt as timestamp) as org_reg_pravo_dt,
        					cast(c.org_reg_egrul_dt as timestamp) as org_reg_egrul_dt,
        					cast(c.org_reg_rosstat_dt as timestamp) as org_reg_rosstat_dt,
                  cast(c.org_reg_eks_dt as timestamp) as	org_reg_eks_dt,			                   --@@@NEW_5 дата регистрации из  ЕКС
        					cast(c.ogrn_egrul_dt as timestamp) as ogrn_egrul_dt,
        					cast(c.ogrn_rosstat_dt as timestamp) as ogrn_rosstat_dt,
        					cast(c.ogrn_nvl_dt as timestamp) as ogrn_nvl_dt,
        					cast(c.min_reg_dt as timestamp) as min_reg_dt,
        					c.org_segment_name,
        					c.org_subsegment_name,
        					c.org_category_name,
        					c.org_opf_crm_name,
        					c.org_opf_pravo_name,
        					c.org_opf_egrul_cd,
        					c.opf_name_egrul,
        					c.opf_cd_rosstat,
        					c.is_stop_list_flag,
        					c.org_industry_name,
        					c.org_kategory_name,
        					c.card_org_status_name,
        					c.kind_activity_name,
        					c.kind_activity_cd,
        					c.belonging_name,
        					c.org_ispartner_cd,
        					c.org_priority_cd,
        					c.risk_segment_name,
        					c.risk_segment_dt,
        					c.ul_status_name,
        					c.ul_activity_stop_method_name,
        					c.open_acct_cnt,
        					c.open_cred_acct_cnt,
        					c.open_guarantee_agrmnt_cnt,
        					c.cred_crm_cnt ,
        					c.AGREEM_FRAME_eks_cnt,
        					c.revenue_year,
        					c.revenue_amt,
        					c.check_income_flag,
        					c.is_restr_flag,
        					c.ru_file_id,
        					c.RATING_APPR_DT,
        					c.RATING_MAIN_PROCESS,
        					c.PD_MAIN_PROCESS,
        					c.RATING_MODEL_NAME,
        					c.RATING_MAIN_PROCESS_PRICE,
        					c.RATING_MAIN_PROCESS_REZERV,
        					cast(c.fok_capit_dt as timestamp) as fok_capit_dt,	-- @@@NEW_3 Озеров: капитал
        					c.fok_capit,																	-- @@@NEW_3 Озеров: капитал
        					c.ros_capit,																	-- @@@NEW_3 Озеров: капитал
                	case  													-- @@@NEW_4  Озеров: Кредитующимися считаем те компании, у которых в CRM имеется рейтинг в статусе "Актуальный", с датой утверждения не старше, чем текущая дата - 6 месяцев
                	when c.RATING_APPR_DT>=add_months(current_date(),-6) then true
                	else false
                	end is_cred_flag
        from $Node1t_team_k7m_aux_d_basis_clientIN c
        $limitString
        where
        	case
        		when revenue_year is null then false                                                   -- @@@NEW_1  Озеров: проверяем, что есть отчетность за нужный год
        		when cast(substr(min_reg_dt,1,4) as int) >= revenue_year  then false   	   -- @@@NEW_1  Озеров: Проверяем, что дата перв. рег. компании ранее начала года отчетности Росстат
        		else true end
        	and revenue_amt>0  																			-- @@@NEW_2  Озеров: проверяю, что выручка больше 0
        	and	case 																			       -- @@@NEW_3 Озеров, Кульпина: Проверяем, дату реорганизации по ОГРН
        		when is_restr_flag=false then true
        		when is_restr_flag=true and (cast(substr(coalesce(org_ogrn_egrul_num, org_ogrn_crm_num),2,2) as int) < revenue_year-2000) then true
        		else false end
        	and	case																	-- @@@NEW_3  Озеров: исключаю из Базиса организации по ОКК (виды деятельности в CRM)
        		when
        		kind_activity_cd in ('1.1','1.2','1.3','1.4','1.5','1.6','1.7','1.8','1.9','1.10','1.12','1.13','1.16','1.17','1.20','1.21','1.22','43.14','46.61',
        		'61.11','62.11','62.12','63.3','63.11','63.15','64.11','65.11','71.10','71.14','71.16','71.19','71.22',
        		'72.1','72.2','72.3','72.4','72.5','72.6','72.7','72.8','72.9','72.10','72.12','72.13','72.17','72.20','72.21','72.22',
        		'73.1','73.2','73.3','73.4','73.5','73.6','73.7','73.8','73.9','73.10','73.13','73.14','73.16','73.17','73.19','73.20','73.21','73.22',
        		'74.7','74.8','74.9','74.10','74.12','74.13','74.20','74.21','74.22',
        		'75.1','75.2','75.3','75.4','75.5','75.6','75.7','75.8','75.9','75.10','75.12','75.13','75.20','75.21','75.22',
        		'101.18','102.18','102.23','103.18','104.18','111.19','112.19','113.19'
        		,'13.10','13.11','13.15','13.18','13.19','13.21','13.22','13.23','51.10','51.11','51.18','51.21','51.22')
        		then false
        		when
        		kind_activity_cd is null and
        		(
        		lower(kind_activity_name) like '%ниокр%' or
        		lower(kind_activity_name) like '%оптовая% и% розничная% торговля% автотранспортными% средствами% и% мотоциклами%(в том числе постпродажное облуживание)%' or
        		lower(kind_activity_name) like '%девелопмент%жилой%недвижимости%' or
        		lower(kind_activity_name) like '%девелопмент% коммерческой%(нежилой)% недвижимости%' or
        		lower(kind_activity_name) like '%девелопмент% промышленной/производственной% недвижимости%(индустриальные парки)%' or
        		lower(kind_activity_name) like '%прочие%сделки%девелопмента%' or
        		lower(kind_activity_name) like '%операции% с% недвижимым% имуществом% за% вознаграждение% или% на% договорной% основе%' or
        		lower(kind_activity_name) like '%гражданское строительство%' or
        		lower(kind_activity_name) like '%промышленное строительство%' or
        		lower(kind_activity_name) like '%строительство инженерной инфраструктуры%' or
        		lower(kind_activity_name) like '%строительство транспортной инфраструктуры%' or
        		lower(kind_activity_name) like '%прочее строительство%' or
        		lower(kind_activity_name) like '%банковская деятельность%' or
        		lower(kind_activity_name) like '%лизинг%' or
        		lower(kind_activity_name) like '%страховая%деятельность%' or
        		lower(kind_activity_name) like '%инвестиционная%деятельность%' or
        		lower(kind_activity_name) like '%федеральные%органы власти%' or
        		lower(kind_activity_name) like '%субъекты рф%' or
        		lower(kind_activity_name) like '%муниципальные%образования%'
        		)
        		then false
        		else true end
        	and	case 																				   -- @@@NEW_3 Озеров: Проверяем, статус из ЕГРЮЛ
        		when lower(ul_status_name) like '%реорганизац%'  then false
        		when lower(ul_status_name) like '%ликвидац%' then false
        		else true end
        	and case    																					-- @@@NEW_3  фильтр: организации с нужной ОПФ (озеров 2103)
        		when (lower(opf_name_egrul) like 'обществ% с% ограничен% ответственност%' or
        		lower(opf_name_egrul) like '%акционерн% обществ%') then true
        		when opf_name_egrul is null and
        		(lower(org_opf_pravo_name)like 'обществ% с% ограничен% ответственност%' or
        		lower(org_opf_pravo_name) like '%акционерн% обществ%') then true
        		when opf_name_egrul is null and org_opf_pravo_name is null and
        		lower(org_opf_crm_name) in ('ао','ооо','пао','зао','оао') then true
        		else false end
         	and case  															-- @@@NEW_4  Озеров: фильтр по выручке - включаем в базис если клиент кредитующийся(по рейтингу ) или имеет выручку более 400млн, остальных исключаем
         		when c.RATING_APPR_DT>=add_months(current_date(),-6) then true
         		when c.revenue_amt>=400000000 then true
         		else false
         		end

       """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_clientOUT")

    logInserted()
    logEnd()
  }

}


