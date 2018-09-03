package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisIntEgrulClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  //-----------------------Integrum_pravo-----------------
  val Node1t_team_k7m_aux_d_basis_integr_egrulIN = s"${Stg0Schema}.int_ul_organization_egrul"
  val Node2t_team_k7m_aux_d_basis_integr_egrulIN = s"${DevSchema}.basis_client_crm"
  val Nodet_team_k7m_aux_d_basis_integr_egrulOUT = s"${DevSchema}.basis_integr_egrul"
  val dashboardPath = s"${config.auxPath}basis_integr_egrul"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_integr_egrulOUT //витрина
  override def processName: String = "Basis"

  def DoBasisIntEgrul()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_integr_egrulOUT).setLevel(Level.WARN)
    logStart()

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
         select   t2.org_id, --уникальный id в core_internal_integrum.ul_organization_egrul
                  t2.ul_inn, --ИНН организации
                  t2.ul_kpp, --КПП организации
                  t2.ul_ogrn, --ОГРН организации
                  t2.ul_national_nm, --Наименование на национальном языке
                  t2.ul_firm_nm, --Фирменное наименование
                  t2.ul_full_nm, --Полное наименование
                  t2.ul_kopf_cd, --Код КОПФ
                  t2.ul_kopf_nm, --Наименование ОПФ
                  t2.ul_status_nm, --Статус организации
                  t2.effectivefrom,
                  t2.ul_status_dt, --Дата установления статуса организации
                  t2.ul_active_flg, --Является ли компания или ИП действующей
                  t2.ul_activity_stop_dt, --Дата прекращения деятельности организации
                  t2.ul_activity_stop_method_nm, --Метод прекращения деятельности
                  t2.ul_reg_first_dt, --Дата первичной регистрации организации
                  t2.ul_reg_ogrn_dt, --Дата присвоения ОГРН организации
                  t2.is_active_in_period, --Флаг актуальности внутри периода
                  t2.ul_slice_id, --id среза
                  t2.effectiveto --Дата закрытия версии
         from (select
                  t1.org_id, --уникальный id в core_internal_integrum.ul_organization_egrul
                  t1.ul_inn, --ИНН организации
                  t1.ul_kpp, --КПП организации
                  t1.ul_ogrn, --ОГРН организации
                  t1.ul_national_nm, --Наименование на национальном языке
                  t1.ul_firm_nm, --Фирменное наименование
                  t1.ul_full_nm, --Полное наименование
                  t1.ul_kopf_cd, --Код КОПФ
                  t1.ul_kopf_nm, --Наименование ОПФ
                  t1.ul_status_nm, --Статус организации
                  t1.effectivefrom,
                  t1.ul_status_dt, --Дата установления статуса организации
                  t1.ul_active_flg, --Является ли компания или ИП действующей
                  t1.ul_activity_stop_dt, --Дата прекращения деятельности организации
                  t1.ul_activity_stop_method_nm, --Метод прекращения деятельности
                  t1.ul_reg_first_dt, --Дата первичной регистрации организации
                  t1.ul_reg_ogrn_dt, --Дата присвоения ОГРН организации
                  t1.is_active_in_period, --Флаг актуальности внутри периода
                  t1.ul_slice_id, --id среза
                  t1.effectiveto, --Дата закрытия версии,
                  row_number() over(partition by t1.ul_inn order by t1.effectivefrom desc) as rn1
             from (select
                  t.org_id, --уникальный id в core_internal_integrum.ul_organization_egrul
                  t.ul_inn, --ИНН организации
                  t.ul_kpp, --КПП организации
                  t.ul_ogrn, --ОГРН организации
                  t.ul_national_nm, --Наименование на национальном языке
                  t.ul_firm_nm, --Фирменное наименование
                  t.ul_full_nm, --Полное наименование
                  t.ul_kopf_cd, --Код КОПФ
                  t.ul_kopf_nm, --Наименование ОПФ
                  t.ul_status_nm, --Статус организации
                  t.effectivefrom,
                  t.ul_status_dt, --Дата установления статуса организации
                  t.ul_active_flg, --Является ли компания или ИП действующей
                  t.ul_activity_stop_dt, --Дата прекращения деятельности организации
                  t.ul_activity_stop_method_nm, --Метод прекращения деятельности
                  t.ul_reg_first_dt, --Дата первичной регистрации организации
                  t.ul_reg_ogrn_dt, --Дата присвоения ОГРН организации
                  t.is_active_in_period, --Флаг актуальности внутри периода
                  t.ul_slice_id, --id среза
                  t.effectiveto, --Дата закрытия версии
                  row_number() over(partition by t.org_id order by t.effectiveto desc) as rn
             from (select
                  o.egrul_org_id as org_id, --уникальный id в core_internal_integrum.ul_organization_egrul
                  o.ul_inn, --ИНН организации
                  o.ul_kpp, --КПП организации
                  o.ul_ogrn, --ОГРН организации
                  o.ul_national_nm, --Наименование на национальном языке
                  o.ul_firm_nm, --Фирменное наименование
                  o.ul_full_nm, --Полное наименование
                  o.ul_kopf_cd, --Код КОПФ
                  o.ul_kopf_nm, --Наименование ОПФ
                  o.ul_status_nm, --Статус организации
          				cast(o.effectivefrom as timestamp) as effectivefrom,       -- @@@NEW_3 поменяла поле для актуализации
                  cast(o.ul_status_dt as timestamp) as ul_status_dt, --Дата установления статуса организации
                  o.ul_active_flg, --Является ли компания или ИП действующей
                  cast(o.ul_activity_stop_dt as timestamp) as ul_activity_stop_dt, --Дата прекращения деятельности организации
                  o.ul_activity_stop_method_nm, --Метод прекращения деятельности
                  cast(o.ul_reg_first_dt as timestamp) as ul_reg_first_dt, --Дата первичной регистрации организации
                  cast(o.ul_reg_ogrn_dt as timestamp) as ul_reg_ogrn_dt, --Дата присвоения ОГРН организации
                  o.is_active_in_period, --Флаг актуальности внутри периода
                  o.ul_slice_id, --id среза
                  cast(o.effectiveto as timestamp) as effectiveto --Дата закрытия версии
             from $Node1t_team_k7m_aux_d_basis_integr_egrulIN o
             join (select distinct org_inn from $Node2t_team_k7m_aux_d_basis_integr_egrulIN) crm
               on o.ul_inn = crm.org_inn
            where o.is_active_in_period = 'Y') t) t1
            where t1.rn = 1
              and year(t1.effectiveto) = 2999
              and t1.ul_active_flg = true) t2
              where t2.rn1 = 1

    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_integr_egrulOUT")

    logInserted()
    logEnd()
  }

}
