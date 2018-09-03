package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.Utils._

class CluClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
      val DevSchema = config.aux
      val Stg0Schema = config.stg
      val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_CLUIN = s"${DevSchema}.clu_base"
  val Node2t_team_k7m_aux_d_CLUIN = s"${DevSchema}.client_eks_clu"
  val Node3t_team_k7m_aux_d_CLUIN = s"${DevSchema}.client_crm_clu"
  val Node4t_team_k7m_aux_d_CLUIN = s"${Stg0Schema}.int_ul_organization_egrul"
  val Node5t_team_k7m_aux_d_CLUIN = s"${Stg0Schema}.int_ip_organization_egrip"
  val Node6t_team_k7m_aux_d_CLUIN = s"${Stg0Schema}.int_Org_Okved_Egrul_Egrip"
  val Node7t_team_k7m_aux_d_CLUIN = s"${Stg0Schema}.int_ul_organization_rosstat"
  val Node8t_team_k7m_aux_d_CLUIN = s"${DevSchema}.int_fin_stmt_rsbu"
  val Node9t_team_k7m_aux_d_CLUIN = s"${DevSchema}.fok_fin_stmt_rsbu"
  val Node10t_team_k7m_aux_d_CLUIN = s"${DevSchema}.client_credit_clu"
  val Node11t_team_k7m_aux_d_CLUIN = s"${DevSchema}.client_guar_clu"
  val Node12t_team_k7m_aux_d_CLUIN = s"${DevSchema}.client_GEN_AGREEM_FRAME_clu"
  val Node13t_team_k7m_aux_d_CLUIN = s"${DevSchema}.client_crm_KRED_clu"
  val Node14t_team_k7m_aux_d_CLUIN = s"${DevSchema}.fns_clu_reg_address"
  val Node15t_team_k7m_aux_d_CLUIN = s"${DevSchema}.eks_clu_address"
  val Node16t_team_k7m_aux_d_CLUIN = s"${Stg0Schema}.prv_organization"
  val Node17t_team_k7m_aux_d_CLUIN = s"${DevSchema}.client_contact_data"
  val Node18t_team_k7m_aux_d_CLUIN = s"${DevSchema}.clu_assets"
  val Node19t_team_k7m_aux_d_CLUIN = s"${DevSchema}.k7m_client_debt_od"
  val Nodet_team_k7m_aux_d_CLUOUT = s"${MartSchema}.CLU"
  val dashboardPath = s"${config.paPath}CLU"


  override val dashboardName: String = Nodet_team_k7m_aux_d_CLUOUT //витрина
  override def processName: String = "CLU"

  def DoClu(limit: Int)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_CLUOUT).setLevel(Level.WARN)



    val limitString: String = if (limit > 0) {s"limit $limit"} else {""}

    val smartSrcHiveTable_t7 = spark.sql(
      s"""	 select distinct
      cast(clubase.u7m_id as string) as u7m_id, 					/*ID юридического лица 7М*/
      cast(clubase.crm_id as string) as crm_id,  							/*ID клиента в CRM 11.04.2018 Указала в явном виде преобразование к string*/
      case
        when crm.crm_flag_resident is not null
        then crm.crm_flag_resident
        when eks.eks_flag_resident is not null
        then eks.eks_flag_resident
        else null
      end flag_resident,	/*Признак (резидент - не резидент)*/
      clubase.inn,                  /*ИНН клиента*/
      eks.kio as kio,                     /*КИО клиента*/
      eks.org_id,     /*Уникальный идентификатор клиента в ЕКС через все ТБ*/
      '$execId' as exec_id,       /*ID сеанса в OD*/
      crm.crm_uc_id as UC_ID,  												/*ID ЕК (единого клиента)*/
      crm.crm_mg_id as MG_ID,  												/*ID МГ (мета группы)*/
         case
         when clubase.eks_id is not null then eks.ogrn
         when clubase.eks_id is null then coalesce(egrul.ul_ogrn, egrip.ip_ogrn)
         end ogrn,                            /*ОГРН(ОГРНИП) клиента в ЕГРЮЛ,ЕГРИП,ЕКС */
      crm.org_ogrn_s_n, 												/*Код и серия свидетельства ОГРН (СРМ)*/
      coalesce(egrul.ul_tax_doc_series, egrip.ip_tax_doc_series) as ogrn_sn,  /*26.04.18 Добавлено заполнение - Серия свидетельства ОГРН*/
      coalesce(egrul.ul_tax_doc_num, egrip.ip_tax_doc_num) as ogrn_code, 		/*26.04.18 Добавлено заполнение - Код свидетельства ОГРН*/
      cast(cast(clubase.eks_id as decimal(38,0)) as string) eks_id, /*ID клиента в ЕКС 	  10.04.2018 - изменила тип на string*/
      clubase.flag_basis_client,  									/*Признак наличия клиента в базисе: t_team_k7m_aux_d.basis_client*/
         case
         when clubase.eks_id is not null then eks.c_kpp
         when clubase.eks_id is null then egrul.ul_kpp
         end kpp,                                  /*КПП клиента*/
         case
         when clubase.eks_id is not null then eks.okpo
         when clubase.eks_id is null then ros.ul_okpo
         end okpo,                                /*ОКПО*/
         case
         when clubase.eks_id is not null then eks.c_long_name
         when clubase.eks_id is null then coalesce(egrul.ul_full_nm, concat_ws(' ', egrip.ip_last_nm, egrip.ip_first_nm, egrip.ip_middle_nm))
         end full_name,                             /*Полное наименование клиента*/
         case
         when clubase.eks_id is not null then eks.cl_name
         when clubase.eks_id is null then coalesce(egrul.ul_short_nm, concat_ws(' ', egrip.ip_last_nm, egrip.ip_first_nm, egrip.ip_middle_nm))
         end short_name,
      eks.opf_code_new as opf_eks_cd,									/*Код ОПФ (ЕКС)*/
      eks.opf_name as opf_eks_nm,										/*Наименование ОПФ (ЕКС)*/
      crm.org_opf as crm_opf_nm,                                       /*Наименование ОПФ (СРМ) 12.04.2018 Переименовала в crm_opf_nm - после обсуждения с Шиловой*/
      coalesce(eks.opf_code_new, eks.opf_code, egrip.ip_okopf_cd, egrul.ul_kopf_cd) as opf,    /*Код ОПФ (EKS/Integrum) 17.04.18 - поле переименовано (было - opf_cd) и добавлен в условие код первой редакции из ЕКС*/
      coalesce(eks.opf_short_name, egrul.ul_kopf_nm) as opf_nm,               /*Код ОПФ (Integrum) 12.04.2018 - поле добавлено; 17.04.18 - поле переименовано из opf в opf_nm. Поле opf - выше*/
      cast(crm.org_date_reg as timestamp) as reg_date_crm, 					/*Дата гос. регистрации (CRM)*/
      coalesce(cast(egrul.UL_REG_FIRST_DT as timestamp), cast(EGRIP.IP_REG_FIRST_DT as timestamp), cast(ros.ul_reg_first_dt as timestamp)) as reg_date_Integrum, --Дата первичной регистрации (Integrum)
      coalesce(cast(crm.org_date_reg as timestamp),  least(cast(egrul.UL_REG_FIRST_DT as timestamp), cast(EGRIP.IP_REG_FIRST_DT as timestamp), cast(ros.ul_reg_first_dt as timestamp)), cast(prv_org.org_reg_dt as timestamp), eks.reg_date) as reg_date, --080518- Добавлена дата из росстата, Право.ру, EKS  - Дата первичной регистрации
      case
      	when eks.reg_country is null and clubase.flag_basis_client='Y' then 'RUS'
      	else eks.reg_country
      end reg_country, 												 --080518 Скорректирована логика на поле. Страна регистрации
         case
            when fns.is_full_fns_address=1 then fns.address_postalcode
            else eks_r_addr.address_postalcode end
         reg_address_postalcode,                                                                                                         /*Адрес регистрации: Почтовый индекс*/
         case
            when fns.is_full_fns_address=1 then upper(fns.address_regionname)
                           else upper(eks_r_addr.address_regionname)
         end reg_address_regionname,                                                                                                     /*Адрес регистрации: Название региона*/
      "" as reg_address_shortregiontypename,                                                                                             /*Адрес регистрации: Название типа региона*/
     case
          when fns.is_full_fns_address=1 then upper(fns.address_districtname)
          when fns.is_full_fns_address=0 and eks_r_addr.address_districtname is not null then upper(concat(eks_r_addr.address_districtname,' район'))
         end reg_address_districtname,                                                                                                      /*Адрес регистрации: Название района*/
     case
          when fns.is_full_fns_address=1 then upper(fns.address_shortdistricttypename)
          when fns.is_full_fns_address=0 and eks_r_addr.address_districtname is not null then upper('район')
       end reg_address_shortdistricttypename,                                                                                            /*Адрес регистрации: Сокращенное название типа района*/
      upper(fns.address_cityname) as reg_address_cityname,                                                                               /*Адрес регистрации: Название города*/
      fns.address_shortcitytypename as reg_address_shortcitytypename,                                                                    /*Адрес регистрации: Сокращенное название типа города*/
      case
         when fns.is_full_fns_address=1 then upper(fns.address_villagename)
         else upper(eks_r_addr.address_villagename)
      end as reg_address_villagename,                                                                                                    /*Адрес регистрации: Наименование населенного пункта*/
       case
         when fns.is_full_fns_address=1 then upper(fns.address_shortvillagetypename)
         else upper(eks_r_addr.address_shortvillagetypename)
         end as reg_address_shortvillagetypename,                                                                                        /*Адрес регистрации: Сокращенное название населенного пункта*/
      case
         when fns.is_full_fns_address=1 then upper(fns.address_streetname)
         else upper(eks_r_addr.address_streetname)
      end reg_address_streetname,                                                                                                        /*Адрес регистрации: Наименование улицы*/
       case
         when fns.is_full_fns_address=1 then upper(fns.address_shortStreetTypeName)
         else upper(eks_r_addr.address_shortstreettypename)
      end reg_address_shortstreettypename,                                                                                               /*Адрес регистрации: Сокращенное наименование улицы*/
      case
         when fns.is_full_fns_address=1 then upper(fns.address_housenumber)
         else upper(eks_r_addr.address_housenumber)
      end reg_address_housenumber,                                                                                                       /*Адрес регистрации: Номер дома*/
      case
         when fns.is_full_fns_address=0 then eks_r_addr.address_buildingnumber
         else null
      end reg_address_blocknumber,                                                                                                    /*Адрес регистрации: Номер строения*/
      case
         when fns.is_full_fns_address=1 then upper(fns.address_blocknumber)
         else eks_r_addr.address_blocknumber
      end reg_address_buildingnumber,                                                                                                       /*Адрес регистрации: Номер корпуса (блока)*/
      case
         when fns.is_full_fns_address=1 then upper(fns.address_flatnumber)
         else eks_r_addr.address_flatnumber
      end reg_address_flatnumber,
      eks_f_addr.address_postalcode as f_address_postalcode,                                                                             /*Адрес фактического местонахождения:  Почтовый индекс*/
      upper(eks_f_addr.address_regionname) as f_address_regionname,                                                                      /*Адрес фактического местонахождения: Название региона*/
      case
         when upper(eks_f_addr.address_districtname)=upper(eks_f_addr.address_regionname) then null
         else upper(concat(eks_f_addr.address_districtname,' район'))
      end f_address_districtname,                                                                                                        /*Адрес фактического местонахождения: Название района*/
      cast(null as string) as f_address_cityname,										/*Адрес фактического местонахождения: Название города*/
      upper(eks_f_addr.address_villagename) as f_address_villagename,                                                                    /*Адрес фактического местонахождения: Наименование населенного пункта*/
      upper(eks_f_addr.address_shortvillagetypename) as f_address_shortvillagetypename,                                                  /*Адрес фактического местонахождения: Сокращенное название населенного пунка*/
      upper(eks_f_addr.address_streetname) as f_address_streetname,                                                                      /*Адрес фактического местонахождения: Наименование улицы*/
      upper(eks_f_addr.address_shortstreettypename) as f_address_shortstreettypename,                                                    /*Адрес фактического местонахождения: Сокращенное наименоване улицы */
      eks_f_addr.address_housenumber as f_address_housenumber,                                                                           /*Адрес фактического местонахождения: Номер дома*/
      eks_f_addr.address_buildingnumber as f_address_blocknumber,                                                                        /*Адрес фактического местонахождения: Номер строения*/
      eks_f_addr.address_blocknumber as f_address_buildingnumber,                                                                        /*Адрес фактического местонахождения: Номер корпуса (блока)*/
      eks_f_addr.address_flatnumber as f_address_flatnumber,                                                                             /*Адрес фактического местонахождения: Номер квартиры*/
      cl_cont.o_phone as o_phone_phonenumber,                                                                                            /*Телефон организации (городской)*/
      cl_cont.m_phone as m_phone_phonenumber,                                                                                            /*Телефон организации (мобильный)*/
      cl_cont.email,                                                                                                                     /*Адрес электронной почты*/
      --cl_cont.fax,                                                                                                                       /*Факс*/
         cast(null as string) as fax,                                                                                                                       /*Факс*/
      crm.crm_gsz_id,                      /*ID группы CRM*/
      crm.crm_top_gsz_id,                      /*ID верхнеуровневой группы CRM*/ /*10.04.2018 - убрала группу атрибутов ‘flag_%_mark’*/
      crm.org_segment as BSEGMENT,                                    /*бизнес - сегмент (CRM)*/
      crm.org_segment_eng as BSEGMENT_ENG,                            /*бизнес - сегмент (CRM)*/
      crm.risk_segment as rsegment,                                   /*Риск - сегмент (CRM)*/
      crm.kind_activity_cd as okk_cd,									/*Вид деятельности_код (CRM)*/
      crm.kind_activity_name as okk,									/*Вид деятельности_наименование (CRM) 11.04.2018 Поле переименовано: okk_name -> okk*/
      cast(null as string) as SOUN,                   /*Код налоговой инспекции*/
      cast(null as timestamp) as soun_data,           /*Дата регистрации в налоговом органе 10.04.2018 Поле переименовано: soun_date -> soun_data*/
      crm.indust_name as industry,  									/*Основной ОКВЭД (Наименование)(CRM)*/
      crm.indust_code as okved_crm_cd, 								/*Основной ОКВЭД (Код)(CRM)*/
      coalesce(int_egrul_okved.okvd_okved_cd, int_egrip_okved.okvd_okved_cd, crm.indust_code) as  okved,  /*Главный ОКВЭД (Интегрум)_Код 10.04.2018 - Переименовала поле okved_integrum_cd -> okved*/
      crm.tb_vko_id,
      crm.tb_vko_name,
      case
        when (c1.c_cr1>0 or g1.c_gr1>0 or fr1.c_gf1>0 or k1.c_cr_crm1>0) then true
        else false
      end borrower_flag,  											/*Признак действующего заемщика*/
      crm.pd_main_process as pd_main_process_offline,                        /*24.04.18 - теперь поле заполяется. PD по процессной модели*/
      crm.approved_date as rating_appr_dt_offline,                                /*24.04.18 - добавила поле. Дата утверждения рейтинга*/
      crm.rat_rowid as RATING_OFFLINE_CALC_ID,
      crm.rating_main_process as rating_main_process_offline,               /*Рейтинг ЮЛ 11.04.2018_2 Добавила постфикс "_offline"*/
      crm.rating_main_process_price as rating_main_process_price_offline,   /*Рейтинг ЮЛ PRICE 11.04.2018_2 Добавила постфикс "_offline"*/
      crm.rating_main_process_rezerv as rating_main_process_rezerv_offline, /*Рейтинг ЮЛ REZERV 11.04.2018_2 Добавила постфикс "_offline"*/
      crm.rating_model_name as rating_model_name_offline,      /*24.04.18 - добавила поле. Наименование модели*/
      cast(null as string) as rating_online_calc_id,          /*10.04.2018 - убрала группу атрибутов %online%*/
      cast(greatest(f.fin_stmt_start_dt, r.fin_stmt_start_dt) as timestamp) as REP_DATE_OFFLINE, /*Дата отчетности. 10.04.2018 - поле переименовано: rep_date -> rep_date_offline*/
      eks.sbbol_ddog as sbbol_ddog,                             			 /*24.04.18 - поле теперь заполняется. Дата Действующего договора СББОЛ*/
      eks.sbbol_ndog as sbbol_ndog,                                        /*24.04.18 - поле теперь заполняется. № действующего договора СББОЛ*/
      Case
        when length(eks.dep_cd)>15 then substr(eks.dep_cd,1,4)
        Else eks.dep_cd
      End sClir,														/*подразделение банка (ЕКС)*/
      crm.ru_id,                                                		 /*Ид корневой папки Клиента в ЕСМ. 12.04.2018 Изменила название поля с offline_ru_id на ru_id*/
      cast(null as Int) as offline_ru_id,        /*Ид корневой папки Клиента в ЕСМ. 11.04.2018_2 В скрипте на CRM изменила название поля*/
                                               /*11.04.2018_2 незаполняемое поле offline_ru_id удалено*/
      cast(null as Int) as OFFLINE_RU_MARK ,    /*Отклик робота-юриста 10.04.2018 - Тип данных изменен на Int*/
      fns.ogrn as ru_ogrn,                                             /*26.04.18 Теперь поле заполняется  - Робот-юрист: ОГРН/ОГРНИП*/
      fns.kpp as ru_kpp,                                               /*26.04.18 Теперь поле заполняется  - Робот-юрист: КПП*/
      fns.ru_full_name as ru_full_name,                         /*26.04.18 Теперь поле заполняется  - Робот-юрист: Полное наименование*/
      fns.ru_short_name as ru_short_name,                        /*26.04.18 Теперь поле заполняется  - Робот-юрист: Краткое наименование*/
      fns.kodopf as ru_opf,                                            /*26.04.18 Теперь поле заполняется  - Робот-юрист: ОПФ*/
      case
        when ((c1.c_cr1=0 or c1.c_cr1 is null) and (g1.c_gr1=0 or g1.c_gr1 is null) and (fr1.c_gf1=0 or fr1.c_gf1 is null) and (k1.c_cr_crm1=0 or k1.c_cr_crm1 is null)) then true
        else false
      end ben_flag,  													 /*28.04.18 - Скорректировано условие на расчет поля. Флаг: запрашивать бенефициаров*/
      cast(asts.assets as decimal(16,4)) as ASSETS       ,   /*Балансовая стоимость активов на последнюю отчетную дату, руб. 10.04.2018 - Тип данных изменен на decimal(16,4)*/
      cast(cl_debt.client_debt as decimal(16,4)) as ACTIVE_EXP_PL,   /*Сумма действующих кредитных обязательств в СБ с учетом плат и комиссий, руб. 10.04.2018 - Тип данных изменен на decimal(16,4)*/
      cast(null as Decimal(16,4)) as ACTIVE_GUA_PL,   /*Сумма действующих обязательств по полученным гарантиям с учетом плат и комиссий, руб. 10.04.2018 - Тип данных изменен на decimal(16,4)*/
      cast(null as Decimal(16,4)) as ACTIVE_SUR_PL,   /*Сумма действующих выданных поручительств в пользу СБ с учетом плат и комиссий, руб. 10.04.2018 - Тип данных изменен на decimal(16,4)*/
      cast((case when crm.x_nsl='Y' then 1 else 0 end) as decimal(16,4)) as lim_struct                   /*24.05.18 Y/N переводим в 1/0  - Структура лимитов, на которой находится заемщик */
  from      $Node1t_team_k7m_aux_d_CLUIN clubase
  left join $Node2t_team_k7m_aux_d_CLUIN eks on eks.cl_id=clubase.eks_id
  left join (select adr.*
                 from $Node15t_team_k7m_aux_d_CLUIN adr
                 where rn_fact=1 and range_type_fact_adr<>6) eks_f_addr on eks_f_addr.cl_id=eks.cl_id
  left join (select adr.*
                 from $Node15t_team_k7m_aux_d_CLUIN adr
                 where rn_fact=1 and range_type_fact_adr<>5) eks_r_addr on eks_r_addr.cl_id=eks.cl_id
  left join $Node3t_team_k7m_aux_d_CLUIN crm on crm.org_id=clubase.crm_id
  left join
   (select * from
					(select egrul.*,
					 row_number() over (partition by egrul.ul_inn order by effectivefrom desc) as rn
					 from $Node4t_team_k7m_aux_d_CLUIN egrul where year(effectiveto)=2999 and is_active_in_period='Y' and ul_active_flg=true) egu2
				 where egu2.rn=1) egrul on egrul.ul_inn=clubase.inn
  left join
   (select * from
				(select egrip.*,
						row_number() over (partition by egrip.ip_inn order by effectivefrom desc) as rn
				 from $Node5t_team_k7m_aux_d_CLUIN egrip
				 where year(effectiveto)=2999 and is_active_in_period='Y' and ip_active_flg=true) egrp
				 where egrp.rn=1) egrip on egrip.ip_inn=clubase.inn
  left join (select *
				 from $Node6t_team_k7m_aux_d_CLUIN okved
				 where year(effectiveto)=2999 and is_active_in_period='Y'
				 ) int_egrul_okved on int_egrul_okved.egrul_org_id=egrul.egrul_org_id and int_egrul_okved.OKVD_MAIN_FLG=1
  left join (select *
				 from $Node6t_team_k7m_aux_d_CLUIN okved
				 where year(effectiveto)=2999 and is_active_in_period='Y'
				 ) int_egrip_okved on int_egrip_okved.egrip_org_id=egrip.egrip_org_id and int_egrip_okved.OKVD_MAIN_FLG=1
  left join (select * from
				(select rst.*,
						row_number() over (partition by ul_inn order by effectivefrom desc) as rn
				from $Node7t_team_k7m_aux_d_CLUIN rst
				where year(effectiveto)=2999 and is_active_in_period='Y' and egrul_org_id is not null) ros
				where ros.rn=1) ros on ros.ul_inn=clubase.inn and ros.egrul_org_id is not null
  left join (select cust_inn_num, max(fin_stmt_start_dt) as fin_stmt_start_dt from $Node8t_team_k7m_aux_d_CLUIN group by cust_inn_num) f on f.cust_inn_num=clubase.inn
  left join (select org_inn,org_reg_dt,org_phone,org_email from $Node16t_team_k7m_aux_d_CLUIN where year(effectiveto)=2999) as prv_org on prv_org.org_inn=clubase.inn
  left join (select crm_cust_id, max(fin_stmt_start_dt) as fin_stmt_start_dt from $Node9t_team_k7m_aux_d_CLUIN group by crm_cust_id)   r on r.crm_cust_id=clubase.crm_id
  left join (select cl_inn, count(*) as c_cr1 from $Node10t_team_k7m_aux_d_CLUIN  where c_high_level_cr is null group by cl_inn)      c1 on c1.cl_inn=clubase.inn
  left join (select principal_inn, count(*) as c_gr1 from $Node11t_team_k7m_aux_d_CLUIN group by principal_inn )                      g1 on g1.principal_inn=clubase.inn
  left join (select c_client_inn, count(*) as c_gf1 from $Node12t_team_k7m_aux_d_CLUIN group by c_client_inn)                        fr1 on fr1.c_client_inn=clubase.inn
  left join (select org_inn, count(*) as c_cr_crm1 from $Node13t_team_k7m_aux_d_CLUIN group by org_inn)                               k1 on k1.org_inn=clubase.inn
  left join $Node14t_team_k7m_aux_d_CLUIN fns on fns.ogrn=egrul.ul_ogrn   		/*26.07.2018*/
  left join $Node17t_team_k7m_aux_d_CLUIN cl_cont on cl_cont.inn=clubase.inn
  left join $Node18t_team_k7m_aux_d_CLUIN asts on asts.inn=clubase.inn
  left join (select c_client_id,
                    sum(client_debt) as client_debt
               from $Node19t_team_k7m_aux_d_CLUIN
              group by c_client_id)     cl_debt on cl_debt.c_client_id=clubase.eks_id
    $limitString
        """
    ) .replace("[\\n\\r]", " ") //убираем непечатные символы
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_CLUOUT")

    logInserted()

    //---------------------------------------------------------------------
    //--Постусловия на ключи:
    //-- Если один  из запросов ниже возвращает записи - писать ошибку в CUSTOM_LOG и падать с  ошибкой! РАСЧЕТ ПРОДОЛЖАТЬ НЕЛЬЗЯ
    //---------------------------------------------------------------------

    //1.
    val checkStage1 = spark.sql(s"""
    select count(u7m_id) cnt from (select
 u7m_id,
 cast(count(*) as string) cnt
 from $Nodet_team_k7m_aux_d_CLUOUT
group by u7m_id
having count(*)>1) t""").first().getLong(0)

    val flagEx: Boolean = checkStage1 > 0

    if (flagEx) {
      log("CLU",s"Дубли U7M_ID. ${checkStage1.toString}", CustomLogStatus.ERROR)
      throw new IllegalArgumentException(s"Дубли U7M_ID. ${checkStage1.toString}")
    }


  }
}
