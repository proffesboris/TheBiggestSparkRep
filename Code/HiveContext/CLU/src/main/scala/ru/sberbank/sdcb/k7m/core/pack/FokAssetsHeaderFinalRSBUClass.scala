package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class FokAssetsHeaderFinalRSBUClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema: String = config.stg
  val DevSchema: String = config.aux

  val Node1t_team_k7m_aux_d_fok_assets_header_final_rsbuIN = s"${Stg0Schema}.fok_docs_header"
  val Node2t_team_k7m_aux_d_fok_assets_header_final_rsbuIN = s"${Stg0Schema}.fok_docs_data"
  val Nodet_team_k7m_aux_d_fok_assets_header_final_rsbuOUT = s"${DevSchema}.fok_assets_header_final_rsbu"
  val dashboardPath = s"${config.auxPath}fok_assets_header_final_rsbu"

  override val dashboardName: String = Nodet_team_k7m_aux_d_fok_assets_header_final_rsbuOUT //витрина
  override def processName: String = "CLU"

  def DoFokAssetsHeaderFinalRSBU()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_fok_assets_header_final_rsbuOUT).setLevel(Level.WARN)



    val smartSrcHiveTable_t7 = spark.sql(
      s"""select dh.id,                               /*id документа*/
                    dh.docs_status,                      /*статус документа*/
                    cast(dh.year as string) as year,     /*год отчетности*/
                    cast(dh.period as string) as period, /*квартал отчетности*/
                    dh.divid,
                    dd1.ch                               /*единица измерения*/
             from $Node1t_team_k7m_aux_d_fok_assets_header_final_rsbuIN dh
             join (
                 select
                    id,
                    row_number() over (partition by formid, divid, year, period order by case when docs_status = 9 then 1 else 0 end desc, modify_date desc) as rn
                 from $Node1t_team_k7m_aux_d_fok_assets_header_final_rsbuIN
                 where formid in ('SB0121')
                  ) mv on (dh.id = mv.id and mv.rn = 1)
            join (
                  select *
                  from $Node2t_team_k7m_aux_d_fok_assets_header_final_rsbuIN dd
                  where dd.field_name = 'MEASURE' and
                        dd.ch is not null and
                        dd.ch in ('млн.', 'тыс.')
        ) dd1 on  dh.id = dd1.docs_header_id
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_fok_assets_header_final_rsbuOUT")

    logInserted()

  }
}
