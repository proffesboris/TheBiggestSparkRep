package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class FokAssetsRosstatClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema: String = config.stg
  val DevSchema: String = config.aux

  val Node1t_team_k7m_aux_d_fok_assets_rosstatIN = s"${Stg0Schema}.int_financial_statement_rosstat"
  val Node2t_team_k7m_aux_d_fok_assets_rosstatIN = s"${Stg0Schema}.int_ul_organization_rosstat"
  val Node3t_team_k7m_aux_d_fok_assets_rosstatIN = s"${DevSchema}.clu_base"
  val Nodet_team_k7m_aux_d_fok_assets_rosstatOUT = s"${DevSchema}.fok_assets_rosstat"
  val dashboardPath = s"${config.auxPath}fok_assets_rosstat"

  override val dashboardName: String = Nodet_team_k7m_aux_d_fok_assets_rosstatOUT //витрина
  override def processName: String = "CLU"

  def DoFokAssetsRosstat()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_fok_assets_rosstatOUT).setLevel(Level.WARN)



    val smartSrcHiveTable_t7 = spark.sql(
      s""" select ros1.*
            from (select ros.*,
                   row_number() over (partition by ul_inn order by report_year desc)  as rn
            from (select
                r.ul_inn,
                f.ul_org_id,                               /*FK на id в cb_akm_integrum.ul_organization_rosstat*/
                f.fs_form_num,                             /*Номер формы бухгалтерской отчетности*/
                f.fs_column_nm,                            /*Столбец формы*/
                f.fs_line_num,                             /*Номер строки*/
                f.fs_line_cd,                              /*Код строки*/
                f.fs_line_desc,                            /*Описание строки*/
                f.period_nm,                               /*Период отчетности*/
                cast(concat(regexp_replace(f.period_nm, '[^0-9]',''),'-12-31 04:00:00') as timestamp) as report_year,  /*Год отчетности*/
                f.fs_value,                               /*Значение*/
                f.fs_unit                                 /*Единица измерения*/
            from $Node1t_team_k7m_aux_d_fok_assets_rosstatIN f
            join (select * from
                (select rst.*,
                        row_number() over (partition by ul_inn order by effectivefrom desc) as rn
                from $Node2t_team_k7m_aux_d_fok_assets_rosstatIN rst
                where year(effectiveto)=2999 and is_active_in_period='Y' and egrul_org_id is not null) ros
                where ros.rn=1) r on r.ul_org_id=f.ul_org_id
            join $Node3t_team_k7m_aux_d_fok_assets_rosstatIN clubase on clubase.inn=r.ul_inn
            where f.fs_form_num='1' and f.fs_line_cd='P16003') ros) ros1
            where ros1.rn=1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_fok_assets_rosstatOUT")

    logInserted()

  }
}
