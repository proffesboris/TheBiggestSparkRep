package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL5Class (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_docapitIN = s"${config.aux}.trbasis_kras"
  val Node2t_team_k7m_aux_d_crit_docapitIN = s"${config.aux}.basis_client"
   val Nodet_team_k7m_aux_d_crit_docapitOUT = s"${config.aux}.crit_docapit"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_docapitOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_docapit"

  def DoSBL5CritDocapit(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_docapitOUT).setLevel(Level.WARN)
    logStart()


    val createHiveTableStage1 = spark.sql(
      s"""        select    case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn1
                           ,case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn2
                           ,'${dateString}' as dt
                           ,count(id) as quantity
                 from $Node1t_team_k7m_aux_d_crit_docapitIN v
                 where v.predicted_value in ('докапитализация') and v.c_date_prov < '${dateString}'
                    and ((case when v.ktdt = 0 then v.inn_sec else v.inn_st end in (select org_inn_crm_num from $Node2t_team_k7m_aux_d_crit_docapitIN)) or
                                 (case when v.ktdt = 0 then v.inn_st else v.inn_sec end in (select org_inn_crm_num from $Node2t_team_k7m_aux_d_crit_docapitIN)))
                                 and v.inn_st  <> '${SparkMain.innSber}'
                                 and v.inn_sec <> '${SparkMain.innSber}'
                                 and (filt = 0)
                 group by case when v.ktdt = 0 then v.inn_sec else v.inn_st end, case when v.ktdt = 0 then v.inn_st else v.inn_sec end
        """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_docapitOUT")

    logEnd()
  }
}

