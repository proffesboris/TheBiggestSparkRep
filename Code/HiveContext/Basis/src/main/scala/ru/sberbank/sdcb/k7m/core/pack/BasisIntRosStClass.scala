package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisIntRosStClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux

  //-----------------------Integrum_pravo-----------------
  val Node1t_team_k7m_aux_d_basis_integr_rosstatIN = s"${Stg0Schema}.int_ul_organization_rosstat"
  val Node2t_team_k7m_aux_d_basis_integr_rosstatIN = s"${DevSchema}.basis_client_crm"
  val Nodet_team_k7m_aux_d_basis_integr_rosstatOUT = s"${DevSchema}.basis_integr_rosstat"
  val dashboardPath = s"${config.auxPath}basis_integr_rosstat"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_integr_rosstatOUT //витрина
  override def processName: String = "Basis"

  def DoBasisIntRosSt()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_integr_rosstatOUT).setLevel(Level.WARN)
    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
          select t1.org_id_egrul,
                 t1.org_id_rt,
                 t1.ul_inn,
                 t1.ul_ogrn,
                 t1.ul_short_nm,
                 t1.ul_full_nm,
                 t1.ul_okopf_cd,
                 t1.ul_reg_first_dt,
                 t1.ul_reg_ogrn_dt,
                 t1.effectivefrom,
                 t1.effectiveto,
                 t1.load_dt,
                 t1.is_active_in_period
            from
                (select
                   r.egrul_org_id as org_id_egrul,              --FK на cb_akm_integrum.ul_organization_egrul. Ссылка на id организации в ЕГРЮЛ
                    r.ul_org_id as org_id_rt,                   --уникальный id в cb_akm_integrum.ul_organization_rosstat
                    r.ul_inn,                          --ИНН организации
                    r.ul_ogrn,                          --ОГРН организации
                    r.ul_short_nm,                        --Краткое наименование
                    r.ul_full_nm,                        --Полное наименование
                    r.ul_okopf_cd,                          --Код ОПФ
                    cast(r.ul_reg_first_dt as timestamp) as ul_reg_first_dt,   --Дата первичной регистрации организации
                    cast(r.ul_reg_ogrn_dt as timestamp) as ul_reg_ogrn_dt,    --Дата присвоения ОГРН организации
                    cast(r.effectivefrom as timestamp) as effectivefrom,    --Дата открытия версии
                    cast(r.effectiveto as timestamp) as effectiveto,      --Дата закрытия версии
                    cast(r.load_dt as timestamp) as load_dt,          --Дата загрузки или обновления
                    r.is_active_in_period,                    --Флаг актуальности внутри периода
                    row_number() over (partition by r.ul_org_id order by r.effectiveto desc) as rn
                from $Node1t_team_k7m_aux_d_basis_integr_rosstatIN r
              where r.ul_inn in (select distinct org_inn from $Node2t_team_k7m_aux_d_basis_integr_rosstatIN)
                and r.egrul_org_id is not null --условие позволяет исключить все филиалы и обособленные подразделения ЮЛ
                ) t1
                where t1.rn=1 and t1.is_active_in_period='Y'
       """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_integr_rosstatOUT")

    logInserted()
    logEnd()
  }

}
