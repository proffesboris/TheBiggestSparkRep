package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisClient0Class (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_client_crm"
  val Node2t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_client_acc"
  val Node3t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_client_credit"
  val Node4t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_client_guar"
  val Node5t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_integr_pravo"
  val Node6t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_integr_egrul"
  val Node7t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_integr_rosstat2"
  val Node8t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_fin_state_rosstat"
  val Node9t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_integr_predecessor"
  val Node10t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_client_crm_rating"
  val Node11t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_client_crm_kred"
  val Node12t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_client_zalog"
  val Node13t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_client_gen_agreem_frame"
  val Node14t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_client_okved"
  val Node15t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.fok_fin_stmt_rsbu"
  val Node16t_team_k7m_aux_d_basis_client0IN = s"${DevSchema}.basis_client_eks"
  val Nodet_team_k7m_aux_d_basis_client0OUT = s"${DevSchema}.basis_client0"
  val dashboardPath = s"${config.auxPath}basis_client0"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client0OUT //витрина
  override def processName: String = "Basis"

  def DoBasisClient0()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client0OUT).setLevel(Level.WARN)
    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
         select
         org_crm_id,
         org_short_crm_name,
         org_crm_name,
         org_inn_crm_num,
         org_kpp_crm_num,
         org_ogrn_crm_num,
         org_ogrn_egrul_num,
         indust_crm_name,
         indust_crm_sic_cd,
         okved_egrul_name,
         isresident_crm_cd,
         tb_crm_name,
         tb_vko_crm_id,
         tb_vko_crm_name,
         org_reg_crm_dt,
         org_reg_pravo_dt,
         org_reg_egrul_dt,
         org_reg_rosstat_dt,
         org_reg_eks_dt,
         ogrn_egrul_dt,
         ogrn_rosstat_dt,
         ogrn_nvl_dt,
         min_reg_dt,
         org_segment_name,
         org_subsegment_name,
         org_category_name,
         org_opf_crm_name,
         org_opf_pravo_name,
         org_opf_egrul_cd,
         opf_name_egrul,
         opf_cd_rosstat,
         is_stop_list_flag,
         org_industry_name,
         org_kategory_name,
         card_org_status_name,
         kind_activity_name,
         kind_activity_cd,
         belonging_name,
         org_ispartner_cd,
         org_priority_cd,
         risk_segment_name,
         risk_segment_dt,
         ul_status_name,
         ul_activity_stop_method_name,
         open_acct_cnt,
         open_cred_acct_cnt,
         open_guarantee_agrmnt_cnt,
         cred_crm_cnt,
        -- zalog_eks_cnt,
         AGREEM_FRAME_eks_cnt,
         fok_capit_dt,
         fok_capit,
         ros_capit,
         revenue_year,
         revenue_amt,
         check_income_flag,
         is_restr_flag,
         ru_file_id,
         RATING_APPR_DT,
         RATING_MAIN_PROCESS,
         PD_MAIN_PROCESS,
         RATING_MODEL_NAME,
         RATING_MAIN_PROCESS_PRICE,
         RATING_MAIN_PROCESS_REZERV
         from
         (select   c.org_id as org_crm_id ,                                          --уникальный id в core_internal_crm_kb.s_org_ext (CRM)
                   c.org_short_name as org_short_crm_name,                                    --краткое наименование организации (CRM)
                   c.org_name as org_crm_name,                                              --наименование организации (CRM)
                   c.org_inn as org_inn_crm_num,                                               --ИНН (CRM)
                   c.org_kpp as org_kpp_crm_num,                                               --КПП (CRM)
                   c.org_ogrn as org_ogrn_crm_num,                                             --ОГРН (CRM)
                   egr.ul_ogrn as org_ogrn_egrul_num,                                          --@@@NEW_3 ОГРН из ЕГРЮЛ
                   c.indust_name as indust_crm_name,                                         --Наименование отрасли (основного ОКВЭД) (CRM)
                   c.indust_code as indust_crm_sic_cd,                                    --Код отрасли (основного ОКВЭД) (CRM)
                   okved.okveds_egrul_name as okved_egrul_name,                            --Код отрасли (основного ОКВЭД) (Интегрум ЕГРЮЛ)
                   c.isresident as isresident_crm_cd,                                     --Параметр резидентности. Доступные значения:  Резидент, не работает в РФ;  Нерезидент, не работает в РФ;  Нерезидент, работает в РФ;  Резидент (CRM)
                   c.tb_name as tb_crm_name,                                      --Подразделение Банка, за которым закреплена Организация (ТБ_ЦА)
                   c.tb_vko_id as tb_vko_crm_id,                                   --ссылка на ТБ ВКО
                   c.tb_vko_name as tb_vko_crm_name,                             --наименование ТБ ВКО
                   c.org_date_reg as org_reg_crm_dt,                                        --Дата гос регистрации организации (CRM)
                   p.org_reg_dt as org_reg_pravo_dt,                                    --Дата гос регистрации организации (PRAVO)
                   egr.ul_reg_first_dt as org_reg_egrul_dt,                              --Дата гос регистрации организации (Integrum.EGRUL)
                   ros.ul_reg_first_dt as org_reg_rosstat_dt,                                --Дата гос регистрации организации (Integrum.ROSSTAT)
                   cast(cl_eks.c_register_date_reestr as timestamp) as org_reg_eks_dt,                      -- @@@NEW_5 0805 - для определения даты регистрации из ЕКС
                   egr.ul_reg_ogrn_dt as ogrn_egrul_dt,                                --Дата присвоения ОГРН (Integrum.EGRUL)
                   ros.ul_reg_ogrn_dt as ogrn_rosstat_dt,                                --Дата присвоения ОГРН (Integrum.ROSSTAT)
         					 nvl(egr.ul_reg_ogrn_dt,ros.ul_reg_ogrn_dt) as ogrn_nvl_dt,							--NEW_3 Озеров: нахожу дату присв.ОГРН - если есть, из Integrum.EGRUL, иначе Integrum.ROSSTAT
         					coalesce (c.org_date_reg, least(egr.ul_reg_first_dt,ros.ul_reg_first_dt),
         					          p.org_reg_dt,cl_eks.c_register_date_reestr,'31.12.9999') as min_reg_dt,     -- @@@NEW_5 Озеров: новый алгоритм по дате регистрации - СРМ, минимум(ЕГРЮЛ,РОССТАТ), PRAVO, EKS
                  -- least(nvl(egr.ul_reg_first_dt, '31.12.9999'), nvl(ros.ul_reg_first_dt, '31.12.9999'), nvl(c.org_date_reg, '31.12.9999'), nvl(p.org_reg_dt, '31.12.9999')) as min_reg_dt,    --Минимальная дата первичной регистрации по данным CRM, Integrum.EGRUL, Integrum.ROSSTAT, Pravo (CRM)
                   c.org_segment as org_segment_name,                                        --Сегмент. Крупнейшие, Крупные, Средние , Малые, Микро (CRM)
                   c.org_subsegment as org_subsegment_name,                                   --Возможные значения: Гос.органы, Фин.институт (CRM)
                   c.org_category as org_category_name,                                        --Примеры значений: «Субъект», «Муницип. Образ», «Корп. Клиент», «Исполн.орган» (CRM)
                   c.org_opf as org_opf_crm_name,                                     --ОПФ (CRM)
                   p.opf_nm as org_opf_pravo_name,                                     --ОПФ (Pravo)
                   egr.ul_kopf_cd as org_opf_egrul_cd,                                   --Код ОПФ (ЕГРЮЛ)
                   egr.ul_kopf_nm as opf_name_egrul,                                   --ОПФ (ЕГРЮЛ)
                   ros.ul_okopf_cd as opf_cd_rosstat,                                   --Код ОПФ (Росстат)
                   c.is_stop_list as is_stop_list_flag,                                            --Признак вхождения в стоп-лист (CRM)
                   c.org_industry as org_industry_name,                                              --Отрасль (CRM)
                   c.org_kategory as org_kategory_name,                                          --Филиал, Холдинг, Юр. Лицо, ИП, Территория (CRM)
                   c.card_org_status as card_org_status_name,                                       --Статус карточки организации (CRM)
                   c.kind_activity_name as kind_activity_name,                                       --Наименование вида деятельности (CRM)
                   c.kind_activity_cd as kind_activity_cd,                                        --Код вида деятельности (CRM)
                   c.belonging as belonging_name,                                          --Принадлежность контрагента. Пример значений: «СКБ-Средние», «СКБ-Крупные», «УМБ-Микро» и т.д. (CRM)
                   c.org_ispartner as org_ispartner_cd,                                     --Тип сотрудничества. Варианты: Клиент, Не клиент (CRM)
                   c.org_priority as org_priority_cd,                                --Возможные значения: A, B, C, D. Заполняется посредством механизма импорта данных по приоритезации (CRM)
                   c.risk_segment as risk_segment_name,                                      --Риск - сегмент (CRM)
                   c.risk_segment_date as risk_segment_dt,                                     --Дата установления риск - сегмента (CRM)
                   egr. ul_status_nm as ul_status_name,                                 --Статус организации (EGRUL)
                   egr. ul_activity_stop_method_nm as ul_activity_stop_method_name,                      --Метод прекращения деятельности (EGRUL)
                   f.c_acc as open_acct_cnt,                                    --Количество открытых расчетных счетов (EKS)
                   d.c_cr as open_cred_acct_cnt,                                  --Количество действующих кредитных договоров (EKS)
                  g.c_gr as open_guarantee_agrmnt_cnt,                                --Количество действующих договоров о банковской гарантии(EKS)
                   crm_cr.c_cr as cred_crm_cnt ,                                    --  Количество действующих кредитных продуктов в СРМ
                  -- zalog.c_cr as zalog_eks_cnt,                                     -- Количество договоров обеспечения в ЕКС
                   eks_ag_fr.c_cr as AGREEM_FRAME_eks_cnt ,                            --    Количетво ген.согл. на РКЛ в ЕКС
         fok_fin_st.fin_stmt_start_dt as fok_capit_dt,														-- @@@NEW_3 Озеров: капитал
         fok_fin_st.fin_stmt_1300_amt as fok_capit,															-- @@@NEW_3 Озеров: капитал
         fin_st_cap.fs_value as ros_capit,                                   -- @@@NEW_3 Озеров: капитал
                  fin_st.year_st as revenue_year,                                          --NEW_1  Озеров Год отчетности
                   fin_st.fs_value as revenue_amt,                                --@@@NEW_3 Озеров: убрала деление на 1000000 Выручка за Год отчетности (Integrum.ROSSTAT)
                   case
                     when d.c_cr is null and g.c_gr is null and fin_st.fs_value<400000000 then false
                     else true
                   end check_income_flag,                                --Проверка объема выручки для некридитующихся клиентов
                   case
                     when pr.ul_inn is not null then true
                     else false
                   end is_restr_flag,                                  --Проверка реструктуризации
                   c.ru_file_id,                                      -- Идентификация для робота-юриста
                  rating.RATING_APPR_DT,                                     --  дата утверждения  (рейтинг срм, спирин)
                   rating.RATING_MAIN_PROCESS,                                --  (рейтинг срм, спирин)
                   rating.PD_MAIN_PROCESS,                                    -- PD TTC  (рейтинг срм, спирин)
                   rating.RATING_MODEL_NAME,                                     --  Модель расчета   (рейтинг срм, спирин)
                   rating.RATING_MAIN_PROCESS_PRICE,                              --  Рейтинг PIT  (рейтинг срм, спирин)
                   rating.RATING_MAIN_PROCESS_REZERV                              --Рейтинг REZERV   (рейтинг срм, спирин)
               from $Node1t_team_k7m_aux_d_basis_client0IN c
               join (select inn, count(acc_num) as c_acc from $Node2t_team_k7m_aux_d_basis_client0IN group by inn having count(acc_num)>0) f on f.inn=c.org_inn                  --Фильтруем только клиентов, у которых есть открытые расчетные счета
               left join (select cl_inn, count(cred_id) as c_cr from $Node3t_team_k7m_aux_d_basis_client0IN  --where c_high_level_cr is null --NEW_3 where c_high_level_cr is null -- перенесла в ЕКС
               group by cl_inn ) d on d.cl_inn=c.org_inn              --Дополняем выборку информацией по количеству действующих кредитных договоров у клиента
               left join (select principal_inn, count(num_dog) as c_gr from $Node4t_team_k7m_aux_d_basis_client0IN group by principal_inn ) g on g.principal_inn=c.org_inn  --Дополняем выборку информацией по количеству действующих банковских гарантий у клиента
               left join $Node5t_team_k7m_aux_d_basis_client0IN p on p.org_inn=c.org_inn
               left join $Node6t_team_k7m_aux_d_basis_client0IN egr on egr.ul_inn=c.org_inn
               left join $Node7t_team_k7m_aux_d_basis_client0IN ros on ros.ul_inn=c.org_inn
               --left join (select ul_inn, fs_value from $Node8t_team_k7m_aux_d_basis_client0IN where lower(period_nm)='2016 год' and fs_form_num=2 and lower(fs_line_cd)='p21103' and lower(fs_column_nm)='за отч. период')   --NEW_1  Озеров выручка по клиенту из бухгалтерской отчетности, предоставляемой Росстатом. Год отчетности = T-2, если текущая дата до 01.11.T, =T-1 если текущая дата 01.11.Т-31.12.T
               left join (select ul_inn, fs_value,                 --NEW_1  Озеров
                                 cast(regexp_replace(lower(period_nm),' год','') as int) year_st,      --NEW_1  Озеров
                                 cast(year(current_date())as int) - cast(regexp_replace(lower(period_nm),' год','') as int) razn_y,            --NEW_1  Озеров
                                 case                                                       --NEW_1  Озеров
                                   when cast(month(current_date())as int)<11 then 2                             --NEW_1  Озеров
                                   else 1                                                    --NEW_1  Озеров
                                 end razn_y_fl                                                  --NEW_1  Озеров
                          from $Node8t_team_k7m_aux_d_basis_client0IN                                    --NEW_1  Озеров
                          where fs_form_num=2 and lower(fs_line_cd)='p21103' and lower(fs_column_nm)='за отч. период') fin_st on fin_st.ul_inn=c.org_inn and fin_st.razn_y_fl=fin_st.razn_y  --Вытаскиваем выручку по клиенту за 2016 год из бухгалтерской отчетности, предоставляемой Росстатом
               left join (select ul_inn, fs_value,                                             -- @@@NEW_3  Озеров
                              cast(regexp_replace(lower(period_nm),' год','') as int) year_st,                        -- @@@NEW_3  Озеров
                              cast(year(current_date())as int) - cast(regexp_replace(lower(period_nm),' год','') as int) razn_y,            -- @@@NEW_3  Озеров
                              case                                                       -- @@@NEW_3  Озеров
                                when cast(month(current_date())as int)<11 then 2                              -- @@@NEW_3  Озеров
                                else 1                                                    -- @@@NEW_3  Озеров
                              end razn_y_fl                                                  --@@@NEW_3  Озеров
                            from $Node8t_team_k7m_aux_d_basis_client0IN                                    -- @@@NEW_3  Озеров
                            where fs_form_num= '1' and cast(fs_line_num as int)=1300 and lower(fs_column_nm)='на дату отч. периода') fin_st_cap
                                on fin_st_cap.ul_inn=c.org_inn and fin_st_cap.razn_y_fl=fin_st_cap.razn_y -- @@@NEW_3 Озеров - капитал
               left join $Node9t_team_k7m_aux_d_basis_client0IN pr on pr.ul_inn=c.org_inn
               left join $Node10t_team_k7m_aux_d_basis_client0IN rating on rating.object_id = c.org_id  --рейтинги из CRM
               left join (select org_id, count(org_id) as c_cr from $Node11t_team_k7m_aux_d_basis_client0IN group by org_id) crm_cr on crm_cr.org_id = c.org_id    -- кредитные продукты из CRM
               --left join (select c_client_inn, count(c_client_inn) as c_cr from $Node12t_team_k7m_aux_d_basis_client0IN group by c_client_inn ) zalog on zalog.c_client_inn = c.org_inn      -- подключаю залоги из ЕКС
               left join (select c_client_inn, count(c_client_inn) as c_cr from $Node13t_team_k7m_aux_d_basis_client0IN group by c_client_inn ) eks_ag_fr on eks_ag_fr.c_client_inn=c.org_inn                           -- подключаю ВРКЛ из ЕКС
              -- left join $Node14t_team_k7m_aux_d_basis_client0IN okved on okved.org_crm_id = c.org_id and okved.okveds_egrul_flag = 1
               left join (select crm_cust_id, fin_stmt_1300_amt, fin_stmt_start_dt
                          from (select crm_cust_id,
                                       fin_stmt_1300_amt,
                                       fin_stmt_start_dt,
                                       row_number() over(partition by crm_cust_id order by fin_stmt_start_dt desc) as rn -- @@@NEW_3  Озеров
                                  from $Node15t_team_k7m_aux_d_basis_client0IN)
                         where rn = 1) fok_fin_st	on c.org_id=fok_fin_st.crm_cust_id													-- @@@NEW_3  Озеров
               left join (select org_crm_id,okveds_egrul_name,okveds_egrul_flag
                                    from
                                          (select org_crm_id,okveds_egrul_name,okveds_egrul_flag,
                                                 row_number() over(partition by org_crm_id order by okveds_egrul_flag desc) as rn
                                           from $Node14t_team_k7m_aux_d_basis_client0IN where okveds_egrul_flag = 1)
                                     where rn=1) okved on okved.org_crm_id = c.org_id
               left join (select t.c_inn,
                                 t.c_register_date_reestr,
                                 row_number() over (partition by t.c_inn order by t.c_register_date_reestr) as rn
                          from (select c_inn, c_register_date_reestr
                                from $Node16t_team_k7m_aux_d_basis_client0IN
                                where c_register_date_reestr is not null) t
                          ) cl_eks on cl_eks.c_inn = c.org_inn and cl_eks.rn = 1                        -- @@@NEW_5 0805 - для определения даты регистрации из ЕКС
               )
       """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client0OUT")

    logInserted()
    logEnd()
  }

}
