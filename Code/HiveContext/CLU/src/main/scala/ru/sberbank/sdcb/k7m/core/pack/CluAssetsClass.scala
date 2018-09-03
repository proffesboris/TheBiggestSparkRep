package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class CluAssetsClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema: String = config.stg
  val DevSchema: String = config.aux

  val Node1t_team_k7m_aux_d_clu_assetsIN = s"${DevSchema}.fok_assets"
  val Node2t_team_k7m_aux_d_clu_assetsIN = s"${DevSchema}.clu_assets_rosstat"
  val Nodet_team_k7m_aux_d_clu_assetsOUT = s"${DevSchema}.clu_assets"
  val dashboardPath = s"${config.auxPath}clu_assets"

  override val dashboardName: String = Nodet_team_k7m_aux_d_clu_assetsOUT //витрина
  override def processName: String = "CLU"

  def DoCluAssets()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clu_assetsOUT).setLevel(Level.WARN)



    val smartSrcHiveTable_t7 = spark.sql(
      s"""select coalesce(f.inn, r.ul_inn) as inn,     /*ИНН*/
                  f.fin_stmt_end_quart,                 /*последнее число месяца, соответствующего кварталу отчетности (ФОК)*/
                  f.nm as f_assets,                     /*Активы (по данным ФОК)*/
                  f.head_measure,                       /*единица измерения (по данным ФОК)*/
                  r.period_nm,                          /*отчетный период (год) (Росстат)*/
                  r.report_year,                        /*год отчетности (Росстат)*/
                  r.fs_value,                           /*Активы (по данным Росстат)*/
                  r.fs_unit,                            /*единица измерения (по данным ФОК)*/
                  case
                     when f.fin_stmt_end_quart>r.report_year or r.report_year is null then cast(f.nm as double)*1000
                     else cast(r.fs_value as double)
                  end assets,                           /*Активы по данным ФОК/Росстат, руб. - выбирается наиболее свежая отчетность*/
                  case
                     when f.fin_stmt_end_quart>r.report_year or r.report_year is null then f.fin_stmt_end_quart
                     else r.report_year
                  end assets_period                    /*Год отчетности, за который взяты Активы*/
           from $DevSchema.fok_assets f
           full join $DevSchema.clu_assets_rosstat r on f.inn=r.ul_inn
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clu_assetsOUT")

    logInserted()

  }
}
