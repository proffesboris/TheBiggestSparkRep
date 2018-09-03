package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClientCrmCluClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_s_org_ext"
  val Node2t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_s_org_ext_x"
  val Node3t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_s_org_ext2_fnx"
  val Node4t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_CX_RATING"
  val Node5t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_CX_RAT_RAT"
  val Node6t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_CX_RAT_RAT_VAL"
  val Node7t_team_k7m_aux_d_client_crm_cluIN = s"${config.aux}.k7m_GROUP_GSZ"
  val Node8t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_cx_unif_accnt"
  val Node9t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_cx_mg"
  val Node10t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_CX_RATING_MODEL"
  val Node11t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_s_lst_of_val"
  val Node12t_team_k7m_aux_d_client_crm_cluIN = s"${config.aux}.clu_base"
  val Node13t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_s_indust"
  val Node14t_team_k7m_aux_d_client_crm_cluIN = s"${config.stg}.crm_s_postn"
  val Nodet_team_k7m_aux_d_client_crm_cluOUT = s"${config.aux}.client_crm_clu"


  override val dashboardName: String = Nodet_team_k7m_aux_d_client_crm_cluOUT //витрина
  override def processName: String = "CLU"
  
  val dashboardPath = s"${config.auxPath}client_crm_clu"
  
  def DoClientCrmClu()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_client_crm_cluOUT).setLevel(Level.WARN)



    val smartSrcHiveTable_t7 = spark.sql(
      s"""     select ext.row_id as org_id,                                       /*уникальный id в internal_crm_cb_siebel.s_org_ext*/
                      ext_x.attrib_34 as org_short_name,                          /*краткое наименование организации*/
                      ext.name as org_name,                                       /*наименование организации*/
                      ind.name as indust_name,                                    /*Наименование отрасли (основного ОКВЭД)*/
                      ind.sic as indust_code,                                     /*Код отрасли (основного ОКВЭД)*/
                      tb.name as tb_name,                                         /*Подразделение Банка, за которым закреплена Организация (ТБ/ЦА)*/
                      ext_x.row_id as org_x_id,                                   /*уникальный id в s_org_ext_x*/
                      ext_x.sbrf_inn as org_inn,                                  /*ИНН*/
                      ext_x.sbrf_kpp as org_kpp,                                  /*КПП*/
                      ext_x.attrib_46 as org_ogrn,                                /*ОГРН*/
                      ext_x.sbrf_industry as org_industry,                        /*Отрасль*/
                      ext_x.attrib_72 as org_ogrn_s_n,                            /*Серия и номер свидетельства ОГРН*/
                      case
                          when lower(ext_x.attrib_07) rlike "^резидент" then true
                      else false
                      end crm_flag_resident,                                      /*Признак "Резидент"*/
                      ext_x.attrib_07 isresident,                                 /*Параметр резидентности. Доступные значения: • Резидент, не работает в РФ; • Нерезидент, не работает в РФ; • Нерезидент, работает в РФ; • Резидент*/
                      ext_x.attrib_12 as org_date_reg,                            /*Дата гос регистрации организации*/
                      ext2_fnx.attrib_03 as org_segment,                          /*Крупнейшие, Крупные, Средние , Малые, Микро*/
                      segm.name as org_segment_eng,                               /*Бизнес - сегмент на английском*/
                      ext2_fnx.attrib_06 as org_subsegment,                      /*Возможные значения: Гос.органы, Фин.институт*/
                      ext_x.x_category as org_category,                           /*Примеры значений: «Субъект», «Муницип. Образ», «Корп. Клиент», «Исполн.орган»*/
                      ext_x.x_contractor_type as org_type,                        /*Оценочные компании, Юристы, СРО АУ, Коллекторское агентство*/
                      ext_x.attrib_39 as org_opf,                                 /*ОПФ*/
                      ext_x.attrib_35 as org_brand,                               /*Брэнд; слово или группа слов, с которыми ассоциируется организация у конечного потребителя*/
                      ext_x.x_stoplist_flg as is_stop_list,                       /*Признак вхождения в стоп-лист*/
                      ext.ou_type_cd as org_kategory,                             /*Филиал, Холдинг, Юр. Лицо, ИП, Территория*/
                      ext.created as card_org_created_date,                       /*Дата создания карточки организации*/
                      ext.last_upd as card_org_last_upd_date,                     /*Дата последнего обновления карточки организации*/
                      ext.cust_stat_cd as card_org_status,                        /*Статус карточки*/
                      ext.x_kind_activity as kind_activity_name,                  /*Наименование вида деятельности*/
                      ext.x_Branch_Class_Code as kind_activity_cd,                /*Код вида деятельности - бизнес называет ОКК */
                      ext.x_sbrf_rel_dep as belonging,                            /*Принадлежность контрагента. Пример значений: «СКБ-Средние», «СКБ-Крупные», «УМБ-Микро» и т.д.*/
                      ext.main_ph_num,                                            /*Номер телефона*/
                      ext2_fnx.par_row_id as org_fnx_id,                                        /*уникальный id в s_org_ext2_fnx*/
                      ext2_fnx.attrib_04 as org_ispartner,                        /*Тип сотрудничества. Варианты: Клиент, Не клиент*/
                      ext2_fnx.attrib_07 as org_priority,                         /*Возможные значения: A, B, C, D. Заполняется посредством механизма импорта данных по приоритезации*/
                      ext_x.x_risk_cat as risk_segment,                           /*Риск - сегмент*/
                      ext_x.x_risk_cat_dt as risk_segment_date,                   /*Дата установления риск - сегмента*/
                      pos.bu_id as tb_vko_id,                                     /*ссылка на ТБ ВКО*/
                      tb_vko.name as tb_vko_name,                                 /*наименование ТБ ВКО*/
                      ext_x.x_nsl,                                                /*Структура лимитов, на которой находится заемщик*/
                      crm_rating.row_id as rat_rowid,                             /*id расчета рейтинга в CRM*/
                      crm_rating.approved_date,                                   /*дата утверждения расчета рейтинга в CRM*/
                      crm_rating.status_date,                                     /*дата установления статуса рейтинга в CRM*/
                      crm_rating.default_prob as pd_main_process,                 /*PD по процессной модели*/
                      crm_rating.final_rating as rating_main_process,             /*Рейтинг ЮЛ*/
                      r_price.value as rating_main_process_price,                 /*Рейтинг ЮЛ PRICE*/
                      r_rezerv.value as rating_main_process_rezerv,               /*Рейтинг ЮЛ REZERV*/
                      r_model.name as rating_model_name,                          /*Наименование модели*/
                      case
                          when lower(ext2_fnx.attrib_03) in ('средние','крупные','крупнейшие') and ext.X_SBRF_CRED_SCHEME = 'Корпоративный процесс' then ext_x.x_acc_large_file_id
                          when lower(ext2_fnx.attrib_03) in ('малые', 'микро') and ext.X_SBRF_CRED_SCHEME = 'Кредитный конвейер' then ext_x.X_ACC_SMB_FILE_ID
                      end ru_id,                                                   /*Ид корневой папки Клиента в ЕСМ*/
                      ext_x.x_acc_large_file_id as ru_file_id,                     /*Идентификация для робота-юриста*/
                      cast(null as string) as offline_ru_mark,                                       /*Отклик робота-юриста*/
                      group_gsz.id as crm_gsz_id,                                  /*id ГСЗ*/
                      group_gsz.top_id as crm_top_gsz_id,                          /*id родительской ГСЗ*/
                      cx_unif_accnt.mdm_uc_id as crm_uc_id,
                      cx_mg.mdm_id as crm_mg_id
               from $Node12t_team_k7m_aux_d_client_crm_cluIN clubase
               join $Node1t_team_k7m_aux_d_client_crm_cluIN ext on clubase.crm_id=ext.row_id
               join $Node2t_team_k7m_aux_d_client_crm_cluIN ext_x on ext_x.par_row_id=ext.row_id
               join $Node3t_team_k7m_aux_d_client_crm_cluIN ext2_fnx on ext2_fnx.par_row_id=ext.row_id
               left join $Node13t_team_k7m_aux_d_client_crm_cluIN ind on ind.row_id=ext.pr_indust_id
               left join $Node1t_team_k7m_aux_d_client_crm_cluIN tb on tb.row_id=ext.bu_id
               left join $Node14t_team_k7m_aux_d_client_crm_cluIN pos on  pos.row_id=ext.pr_postn_id
               left join $Node1t_team_k7m_aux_d_client_crm_cluIN tb_vko on tb_vko.row_id=pos.bu_id
         left join (select * from
                 (select r.*,
                     row_number() over (partition by object_id order by approved_date desc) rn
                 from $Node4t_team_k7m_aux_d_client_crm_cluIN r
                 where
                   lower(status) = 'актуальный'
                   and project is null              --Для контрагентов может быть один основной рейтинг и несколько проектных. Для основного рейтинг - проект не указывается
                   and approved_date is not null) r1
                   where r1.rn=1) crm_rating on ext.row_id = crm_rating.object_id
         left join (select rr_rowid,rv_rowid,rating_id,name,value from
                 (select
                   rr.row_id as rr_rowid,
                   rv.row_id as rv_rowid,
                   rr.rating_id,
                   rv.name,
                   rv.value,
                   row_number() over (partition by rr.rating_id order by rv.last_upd desc) rn
                 from $Node5t_team_k7m_aux_d_client_crm_cluIN rr
                 JOIN $Node6t_team_k7m_aux_d_client_crm_cluIN rv on (rv.PARENT_ID = rr.ROW_ID and lower(rv.name) = 'rating_price')) r
                 where r.rn=1) r_price on r_price.RATING_ID = crm_rating.ROW_ID
         left join (select rr_rowid,rv_rowid,rating_id,name,value from
           (select
             rr.row_id as rr_rowid,
             rv.row_id as rv_rowid,
             rr.rating_id,
             rv.name,
             rv.value,
             row_number() over (partition by rr.rating_id order by rv.last_upd desc) rn
           from $Node5t_team_k7m_aux_d_client_crm_cluIN rr
           JOIN $Node6t_team_k7m_aux_d_client_crm_cluIN rv on (rv.PARENT_ID = rr.ROW_ID and lower(rv.name) = 'rating_rezerv')
           ) r
         where r.rn=1) r_rezerv on r_rezerv.RATING_ID = crm_rating.ROW_ID
        left  join $Node10t_team_k7m_aux_d_client_crm_cluIN r_model on r_model.row_id = crm_rating.model_id	/*24.04.18 Добавлен джойн*/
        left  join $Node7t_team_k7m_aux_d_client_crm_cluIN group_gsz on group_gsz.org_crm_id=ext.row_id
        left  join $Node8t_team_k7m_aux_d_client_crm_cluIN cx_unif_accnt on cx_unif_accnt.row_id=ext.x_uc_id
        left  join $Node9t_team_k7m_aux_d_client_crm_cluIN cx_mg on cx_mg.row_id=cx_unif_accnt.mg_id
        left  join $Node11t_team_k7m_aux_d_client_crm_cluIN segm on segm.val = ext2_fnx.attrib_03 and segm.active_flg = 'Y' and lower(segm.type) = 'sbrf_segment_type'
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", s"${dashboardPath}")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_client_crm_cluOUT")

    logInserted()

  }
}

