package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisClCrmClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val DevSchema = config.aux

  //------------------CRM-----------------------------
  val Node1t_team_k7m_aux_d_basis_client_crmIN = s"${DevSchema}.basis_org_ext"
  val Node2t_team_k7m_aux_d_basis_client_crmIN = s"${DevSchema}.basis_org_ext_x"
  val Node3t_team_k7m_aux_d_basis_client_crmIN = s"${DevSchema}.basis_org_ext2_fnx"
  val Nodet_team_k7m_aux_d_basis_client_crmOUT = s"${DevSchema}.basis_client_crm"
  val dashboardPath = s"${config.auxPath}basis_client_crm"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_crmOUT //витрина
  override def processName: String = "Basis"

  def DoBasisClCrm()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_crmOUT).setLevel(Level.WARN)

    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
              select  ext.org_id,            --уникальный id в core_internal_crm_kb.s_org_ext
                       ext_x.org_short_name,        --краткое наименование организации
                       ext.org_name,          --наименование организации
                       ext.indust_name,        --Наименование отрасли (основного ОКВЭД)
                       ext.indust_code,        --Код отрасли (основного ОКВЭД)
                       ext.tb_name,          --Подразделение Банка, за которым закреплена Организация (ТБ/ЦА)
                       ext_x.org_x_id,         --уникальный id в s_org_ext_x
                       ext_x.org_inn,           --ИНН
                       ext_x.org_kpp,           --КПП
                       ext_x.org_ogrn,           --ОГРН
                       ext_x.org_industry,         --Отрасль
                       case
                       when lower(ext_x.isresident) rlike "^резидент" then true
                       else false
                       end crm_flag_resident,            --для CLU
                       ext_x.isresident,           --Параметр резидентности. Доступные значения: • Резидент, не работает в РФ; • Нерезидент, не работает в РФ; • Нерезидент, работает в РФ; • Резидент
                       ext_x.org_date_reg,          --Дата гос регистрации организации
                       fnx.org_segment,          --Крупнейшие, Крупные, Средние , Малые, Микро
                       fnx.org_subsegment,       --Возможные значения: Гос.органы, Фин.институт
                       ext_x.org_category,            --Примеры значений: «Субъект», «Муницип. Образ», «Корп. Клиент», «Исполн.орган»
                       ext_x.org_type,                --Оценочные компании, Юристы, СРО АУ, Коллекторское агентство
                       ext_x.org_opf,                --ОПФ
                       ext_x.org_brand,                --Брэнд; слово или группа слов, с которыми ассоциируется организация у конечного потребителя
                       ext_x.is_stop_list,             --Признак вхождения в стоп-лист
                       ext.org_kategory,              --Филиал, Холдинг, Юр. Лицо, ИП, Территория
                       ext.card_org_created_date,      --Дата создания карточки организации
                       ext.card_org_last_upd_date,    --Дата последнего обновления карточки организации
                       ext.card_org_status,           --Статус карточки
                       ext.kind_activity_name,       --Наименование вида деятельности
                       ext.kind_activity_cd,         --Код вида деятельности
                       ext.belonging,              --Принадлежность контрагента. Пример значений: «СКБ-Средние», «СКБ-Крупные», «УМБ-Микро» и т.д.
                       fnx.org_fnx_id,              --уникальный id в s_org_ext2_fnx
                       fnx.org_ispartner,           --Тип сотрудничества. Варианты: Клиент, Не клиент
                       fnx.org_priority,        --Возможные значения: A, B, C, D. Заполняется посредством механизма импорта данных по приоритезации
                       ext_x.risk_segment,            --Риск - сегмент
                       ext_x.risk_segment_date,         --Дата установления риск - сегмента
                      ext_x.ru_file_id,               --Идентификация для робота-юриста
                      ext.tb_vko_id,                  -- ссылка на ТБ ВКО
                       ext.tb_vko_name,                  -- наименование ТБ ВКО
                      ext_x.org_ogrn_s_n,                --для CLU
                      ext_x.okpo_name,                   --для CLU
                      ext_x.okpo_cd,                     --для CLU
                       case
                         when lower(fnx.org_segment) in ('средние','крупные','крупнейшие') and ext.X_SBRF_CRED_SCHEME = 'Корпоративный процесс' then ext_x.ru_file_id
                         when lower(fnx.org_segment) in ('малые', 'микро') and ext.X_SBRF_CRED_SCHEME = 'Кредитный конвейер' then ext_x.X_ACC_SMB_FILE_ID
                       end RU_ID
                   from $Node1t_team_k7m_aux_d_basis_client_crmIN ext
                   join $Node2t_team_k7m_aux_d_basis_client_crmIN ext_x on ext_x.par_row_id_ext_x=ext.org_id
                   join $Node3t_team_k7m_aux_d_basis_client_crmIN fnx on fnx.par_row_id_fnx=ext.org_id
                   where lower(card_org_status)='закреплена' and          --Выбираем только организации, которые уже закреплены за клиентскими менеджерами
                       lower(org_kategory) rlike "^юр" and              --Выбираем только ЮЛ (не их филиалы, не территории и уж тем более не холдинги) - так как кредит выдается только на ЮЛ
                       lower(isresident) rlike "^резидент" and          --Выбираем только организации - резиденты
                       lower(org_segment) in ('средние','крупные','крупнейшие')            --Выбираем нужные сегменты
                      -- and lower(org_opf) in ('ао','ооо','пао','зао','оао')                 --Фильтруем организации с нужной ОПФ
       """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_crmOUT")

    logInserted()
    logEnd()

  }
}
