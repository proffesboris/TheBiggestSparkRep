package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class FokAssetsClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema: String = config.stg
  val DevSchema: String = config.aux

  val Node1t_team_k7m_aux_d_fok_assetsIN = s"${Stg0Schema}.fok_docs_data"
  val Node2t_team_k7m_aux_d_fok_assetsIN = s"${DevSchema}.fok_assets_header_final_rsbu"
  val Node3t_team_k7m_aux_d_fok_assetsIN = s"${DevSchema}.clu_base"
  val Nodet_team_k7m_aux_d_fok_assetsOUT = s"${DevSchema}.fok_assets"
  val dashboardPath = s"${config.auxPath}fok_assets"

  override val dashboardName: String = Nodet_team_k7m_aux_d_fok_assetsOUT //витрина
  override def processName: String = "CLU"

  def DoFokAssets()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_fok_assetsOUT).setLevel(Level.WARN)



    val smartSrcHiveTable_t7 = spark.sql(
      s"""select fok2.*
                 from (select fok1.*,
                              row_number() over (partition by fok1.inn order by fok1.fin_stmt_beg_quart desc) as rn
                        from (select fok.*
                              from (select clubase.crm_id,                /*crm_id организации*/
                                        clubase.inn,                      /*ИНН организации*/
                                        dd.*,
                                        cast(dh.year as int) as fin_stmt_year, /*год отчетности*/
                                        cast(dh.period as int) as fin_stmt_period, /*квартал отчетности*/
                                        dh.docs_status,                 /*статус формы отчетности*/
                                        dh.ch as head_measure,          /*единица изменения*/
                                        case
                                            when dh.period in ('1','2','3','4') and dh.year > '1900' then case
                                                                                            when dh.period = '1' then cast(concat(dh.year,'-01-01 04:00:00') as timestamp)
                                                                                            when dh.period = '2' then cast(concat(dh.year,'-04-01 04:00:00') as timestamp)
                                                                                            when dh.period = '3' then cast(concat(dh.year,'-07-01 04:00:00') as timestamp)
                                                                                            when dh.period = '4' then cast(concat(dh.year,'-10-01 04:00:00') as timestamp)
                                                                                        end
                                            else null
                                        end fin_stmt_beg_quart,          /*1 число месяца - начала отчетного квартала*/
                                        case
                                            when dh.period in ('1','2','3','4') and dh.year > '1900' then case
                                                                                            when dh.period = '1' then cast(concat(dh.year,'-03-31 04:00:00') as timestamp)
                                                                                            when dh.period = '2' then cast(concat(dh.year,'-06-30 04:00:00') as timestamp)
                                                                                            when dh.period = '3' then cast(concat(dh.year,'-09-30 04:00:00') as timestamp)
                                                                                            when dh.period = '4' then cast(concat(dh.year,'-12-31 04:00:00') as timestamp)
                                                                                        end
                                            else null
                                        end fin_stmt_end_quart            /*последнее число месяца, соответствующего кварталу отчетности (ФОК)*/
                                    from $Node1t_team_k7m_aux_d_fok_assetsIN dd
                                    join $Node2t_team_k7m_aux_d_fok_assetsIN dh on dd.docs_header_id = dh.id
                                    join $Node3t_team_k7m_aux_d_fok_assetsIN clubase on clubase.crm_id=dh.divid
                                    where dd.field_name = 'BB1.1600.3' and dh.docs_status = 9) fok
                             where fin_stmt_end_quart<current_date()) fok1
                    ) fok2
              where fok2.rn=1
             order by fok2.crm_id
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_fok_assetsOUT")

    logInserted()

  }
}
