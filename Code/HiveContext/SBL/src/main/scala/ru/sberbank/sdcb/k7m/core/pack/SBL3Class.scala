package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL3Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_nploanIN = s"${config.aux}.trbasis_kras"
  val Node2t_team_k7m_aux_d_crit_nploanIN = s"${config.aux}.basis_client"
  val Node3t_team_k7m_aux_d_crit_nploanIN = s"${config.aux}.int_ip_organization_egrip_sbl"
   val Nodet_team_k7m_aux_d_crit_nploanOUT = s"${config.aux}.crit_nploan"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_nploanOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_nploan"

  def DoSBL3CritnpLoans(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_nploanOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_crit_nploanPrep = Nodet_team_k7m_aux_d_crit_nploanOUT.concat("_Prep")

    val createHiveTableStage1 = spark.sql(
      s"""    select
                                      case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn1
                                     ,case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn2
                                     ,'${dateString}' as dt
                                     , count(v.id) as quantity
         from $Node1t_team_k7m_aux_d_crit_nploanIN v
                 where lower(v.predicted_value) in ('беспроцентный займ') and c_date_prov < '${dateString}'
                             and ((case when v.ktdt = 0 then v.inn_sec else v.inn_st end in (select org_inn_crm_num from $Node2t_team_k7m_aux_d_crit_nploanIN ))
                                                         or
                                 (case when v.ktdt = 0 then v.inn_st else v.inn_sec end in (select org_inn_crm_num from $Node2t_team_k7m_aux_d_crit_nploanIN)))
                                 and v.inn_st  <> '${SparkMain.innSber}'
                                 and v.inn_sec <> '${SparkMain.innSber}'
                                 and (filt = 0)
                 group by (case when v.ktdt = 0 then v.inn_sec else v.inn_st end), case when v.ktdt = 0 then v.inn_st else v.inn_sec end"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_nploanPrep")

    val createHiveTableStage3 = spark.sql(
      s"""
             select inn1,inn2,dt,quantity from $Nodet_team_k7m_aux_d_crit_nploanPrep
             where (     (length(inn1) = 10) or
                         ((length(inn1) = 12) and (inn1 in (select ip_inn from $Node3t_team_k7m_aux_d_crit_nploanIN)))
                     )
                     and
                     ((length(inn2) = 10) or
                      ((length(inn2) = 12) and (inn2 in (select ip_inn from $Node3t_team_k7m_aux_d_crit_nploanIN)))
                     )
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_nploanOUT")

    logEnd()
  }
}
