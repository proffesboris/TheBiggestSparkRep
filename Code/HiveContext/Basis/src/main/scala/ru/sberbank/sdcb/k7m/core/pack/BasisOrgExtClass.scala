package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisOrgExtClass  (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  //------------------CRM-----------------------------
  val Node1t_team_k7m_aux_d_basis_org_extIN = s"${Stg0Schema}.crm_s_org_ext"
  val Node2t_team_k7m_aux_d_basis_org_extIN = s"${Stg0Schema}.crm_s_indust"
  val Node3t_team_k7m_aux_d_basis_org_extIN = s"${Stg0Schema}.crm_s_postn"
   val Nodet_team_k7m_aux_d_basis_org_extOUT = s"${DevSchema}.basis_org_ext"
  val dashboardPath = s"${config.auxPath}basis_org_ext"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_org_extOUT //витрина
  override def processName: String = "Basis"

  def DoBasisOrgExt()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_org_extOUT).setLevel(Level.WARN)

    logStart()

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
                     select      o.row_id                             as org_id,                --уникальный id в internal_crm_cb_siebel.s_org_ext
                                 ind.name                                as indust_name,              --Наименование отрасли (Основной ОКВЭД)
                                 ind.sic                               as indust_code,              --Код отрасли (Основной ОКВЭД (код))
                                 tb.name                            as tb_name,                 --Подразделение Банка, за которым закреплена Организация (ТБ_ЦА)
                                 o.name                                 as org_name,              --наименование организации
                                 o.int_org_flg                          as int_org_flg,           --атрибут для отсева подразделений сбербанка
                                 o.pr_indust_id                         as indust_id,             --id отрасли. FK на s_indust.row_id
                                 o.ou_type_cd                           as org_kategory,          --Филиал, Холдинг, Юр. Лицо, ИП, Территория
                                 o.created                               as card_org_created_date, --Дата создания карточки организации
                                 o.last_upd                               as card_org_last_upd_date,--Дата последнего обновления карточки организации
                                 o.cust_stat_cd                         as card_org_status,       --Статус карточки
                                 o.x_kind_activity                      as kind_activity_name,    --Наименование вида деятельности
                                 o.x_Branch_Class_Code                  as kind_activity_cd,      --Код вида деятельности
                                 o.x_sbrf_fi_flag                       as is_fi,                   --Влияет на права доступа Возможные значения: Y, N, NULL Указывает, что с данной карточкой должны иметь возможность работать сотрудники ЦА, ответственные за международные отношения в рамках процесса «Ведение контактов руководства Банка с Финансовыми институтами и Организациями»
                                 o.x_sbrf_rel_dep                       as belonging,               --Принадлежность контрагента. Пример значений: «СКБ-Средние», «СКБ-Крупные», «УМБ-Микро» и т.д.
                                 o.bu_id                                as tb_id,                 --id подразделения Банка, за которым закреплена Организация (ТБ_ЦА)
                                 o.pr_postn_id                          as vko_id,			      --ссылка на владельца карточки - ВКО
                                 pos.bu_id							                as tb_vko_id,			  --ссылка на ТБ ВКО
                                 tb_vko.name                            as tb_vko_name,			  --наименование ТБ ВКО
                                 o.x_sbrf_cred_scheme                   as x_sbrf_cred_scheme,
                                 o.x_uc_id                              as x_uc_id
                     from      $Node1t_team_k7m_aux_d_basis_org_extIN o
                     left join $Node2t_team_k7m_aux_d_basis_org_extIN ind on ind.row_id=o.pr_indust_id
                     left join $Node1t_team_k7m_aux_d_basis_org_extIN tb on tb.row_id=o.bu_id
                     left join $Node3t_team_k7m_aux_d_basis_org_extIN pos on  pos.row_id=o.pr_postn_id        -- ВКО
                     left join $Node1t_team_k7m_aux_d_basis_org_extIN tb_vko on tb_vko.row_id=pos.bu_id     --ТБ ВКО
                     where o.int_org_flg='N'
      """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_org_extOUT")

    logInserted()
    logEnd()
  }
}
