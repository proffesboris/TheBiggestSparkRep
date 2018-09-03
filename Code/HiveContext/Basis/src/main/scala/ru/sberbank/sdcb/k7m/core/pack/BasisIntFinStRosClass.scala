package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisIntFinStRosClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val DevSchema = config.aux
  val Stg0Schema = config.stg
  //-----------------------Integrum_pravo-----------------
  val Node1t_team_k7m_aux_d_basis_fin_state_rosstatIN = s"${Stg0Schema}.int_financial_statement_rosstat"
  val Node2t_team_k7m_aux_d_basis_fin_state_rosstatIN = s"${DevSchema}.basis_integr_rosstat2"
  val Nodet_team_k7m_aux_d_basis_fin_state_rosstatOUT = s"${DevSchema}.basis_fin_state_rosstat"
  val dashboardPath = s"${config.auxPath}basis_fin_state_rosstat"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_fin_state_rosstatOUT //витрина
  override def processName: String = "Basis"

  def DoBasisFinStRos()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_fin_state_rosstatOUT).setLevel(Level.WARN)
    logStart()


    val smartSrcHiveTable_t7 = spark.sql(
      s"""
                select
                 f.ul_org_id,              --FK на id в cb_akm_integrum.ul_organization_rosstat
                 r.ul_inn,                --ИНН организации
                 r.ul_ogrn,                --ОГРН организации
                 r.ul_short_nm,              --Краткое наименование
                 f.fs_form_num,               --Номер формы бухгалтерской отчетности
                 f.fs_column_nm,              --Столбец формы
                 f.fs_line_num,              --Номер строки
                 f.fs_line_cd,               --Код строки
                 f.fs_line_desc,                --Описание строки
                 f.period_nm,                 --Период отчетности
                 f.fs_value,                 --Значение
                 f.fs_unit                   --Единица измерения
               from $Node1t_team_k7m_aux_d_basis_fin_state_rosstatIN f
               join $Node2t_team_k7m_aux_d_basis_fin_state_rosstatIN r on r.org_id_rt=f.ul_org_id
       """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_fin_state_rosstatOUT")

    logInserted()
    logEnd()
  }

}
