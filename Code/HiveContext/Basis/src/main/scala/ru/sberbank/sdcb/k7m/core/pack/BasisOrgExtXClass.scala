package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisOrgExtXClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val DevSchema = config.aux
  val Stg0Schema = config.stg

  val Node1t_team_k7m_aux_d_basis_org_ext_xIN = s"${Stg0Schema}.crm_s_org_ext_x"
  val Node2t_team_k7m_aux_d_basis_org_ext_xIN = s"${DevSchema}.basis_org_ext"
   val Nodet_team_k7m_aux_d_basis_org_ext_xOUT = s"${DevSchema}.basis_org_ext_x"
  val dashboardPath = s"${config.auxPath}basis_org_ext_x"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_org_ext_xOUT //витрина
  override def processName: String = "Basis"

  def DoBasisOrgExtX()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_org_ext_xOUT).setLevel(Level.WARN)

    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
                         select        t.row_id                         as org_x_id,                 --уникальный id в s_org_ext_x
                                       t.par_row_id                     as par_row_id_ext_x,            --FK на s_org_ext.row_id
                                       t.attrib_34                      as org_short_name,              --краткое наименование организации
                                       t.sbrf_inn                       as org_inn,                   --ИНН
                                       t.sbrf_kpp                       as org_kpp,                   --КПП
                                       t.attrib_46                      as org_ogrn,                   --ОГРН
                                       t.attrib_72                      as org_ogrn_s_n,               --для CLU
         							                 t.attrib_68 						          as okpo_name,					         --для CLU
         							                 t.attrib_49 						          as okpo_cd,						         --для CLU
                                       t.sbrf_industry                  as org_industry,               --Отрасль
                                       t.attrib_07                      as isresident,                 --Параметр резидентности. Доступные значения: Резидент, не работает в РФ; Нерезидент, не работает в РФ; Нерезидент, работает в РФ; Резидент
                                       t.attrib_12                         as org_date_reg,          --Дата гос регистрации организации
                                       t.x_risk_cat                     as risk_segment,              --Риск - сегмент
                                       t.x_risk_cat_dt                 as risk_segment_date,         --Дата установления Риск - сегмента
                                       t.x_category                     as org_category,              --Примеры значений: Субъект, Муницип. Образ, Корп. Клиент, Исполн.орган
                                       t.x_contractor_type              as org_type,                  --Оценочные компании, Юристы, СРО АУ, Коллекторское агентство
                                       t.attrib_39                      as org_opf,                  --ОПФ
                                       t.attrib_35                      as org_brand,                  --Брэнд; слово или группа слов, с которыми ассоциируется организация у конечного потребителя
                                       t.x_stoplist_flg                 as is_stop_list,             --Признак вхождения в стоп-лист
                                       t.x_stoplist_comm                as reason_stop_list,         --Описание причины вхождения в стоп-лист. банкротство, внесение в черную зону, наличие просроченной задолжности, недобросовестные действия заемщика и т.п.
                                       t.x_acc_large_file_id            as ru_file_id,               -- Идентификация для робота-юриста
                                       t.x_acc_smb_file_id              as x_acc_smb_file_id,
                                       t.x_nsl                          as x_nsl                     --18.04.18 - Поле добавлено для CLU - Структура лимитов, на которой находится заемщик
                         from $Node1t_team_k7m_aux_d_basis_org_ext_xIN t
                         inner join $Node2t_team_k7m_aux_d_basis_org_ext_xIN o on t.par_row_id = o.org_id
      """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_org_ext_xOUT")

    logInserted()
    logEnd()
  }
}
