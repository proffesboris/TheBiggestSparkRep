package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class CluAssetsRosstatClass ( val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val StgSchema: String = config.stg
  val DevSchema: String = config.aux

  val Nodet_team_k7m_aux_d_clu_assetsOUT = s"${DevSchema}.clu_assets_rosstat"
  val dashboardPath = s"${config.auxPath}clu_assets_rosstat"

  override val dashboardName: String = Nodet_team_k7m_aux_d_clu_assetsOUT //витрина
  override def processName: String = "CLU"

  def DoCluAssetsRosstat()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clu_assetsOUT).setLevel(Level.WARN)

    val smartSrcHiveTable = spark.sql(
    s"""with rosstat_actual as
       (
           select
                   *
             from
                   (
                       select
                               rst.*,
                               row_number() over (partition by ul_inn order by effectivefrom desc) as rn
                         from
                               $StgSchema.int_ul_organization_rosstat rst
                        where
                               year(effectiveto) = 2999 and
                               is_active_in_period = 'Y' and
                               egrul_org_id is not null
                   ) ros
            where
                   ros.rn = 1
       )
       select
               ros1.*
         from
               (
                   select
                           ros.*,
                           row_number() over (partition by ul_inn order by report_year desc)  as rn
                     from
                           (
                               select
                                       r.ul_inn,
                                       f.ul_org_id,
                                       f.fs_form_num,
                                       f.fs_column_nm,
                                       f.fs_line_num,
                                       f.fs_line_cd,
                                       f.fs_line_desc,
                                       f.period_nm,
                                       cast(concat(regexp_replace(f.period_nm, '[^0-9]',''),'-12-31 04:00:00') as timestamp) as report_year,
                                       f.fs_value,
                                       f.fs_unit
                                 from
                                       $StgSchema.int_financial_statement_rosstat f
                                       join rosstat_actual r on r.ul_org_id=f.ul_org_id
                                       join $DevSchema.clu_base clubase on clubase.inn=r.ul_inn
                                where
                                       f.fs_form_num='1' and
                                       f.fs_line_cd='P16003'
                           ) ros
               ) ros1
        where
               ros1.rn = 1
      """
    )
    smartSrcHiveTable
    .write.format("parquet")
    .mode("overwrite")
    .option("path", dashboardPath)
    .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_assetsOUT")

    logInserted()

  }
}
