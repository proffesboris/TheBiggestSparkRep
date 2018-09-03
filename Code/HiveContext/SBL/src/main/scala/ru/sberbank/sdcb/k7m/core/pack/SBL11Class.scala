package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL11Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_consult_inIN = s"${config.aux}.basis_client"
  val Node2t_team_k7m_aux_d_crit_consult_inIN = s"${config.aux}.trbasis_kras"
  val Node3t_team_k7m_aux_d_crit_consult_inIN = s"${config.aux}.Revenue_Target"
  val Nodet_team_k7m_aux_d_crit_consult_inOUT = s"${config.aux}.crit_consult_in"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_consult_inOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_consult_in"

  def DoSBL11CritConsIn(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_consult_inOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_crit_consult_inPrep = Nodet_team_k7m_aux_d_crit_consult_inOUT.concat("_Prep")

    val createHiveTableStage1 = spark.sql(
      s"""select
           case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn_dt
          ,case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn_kt
           , '${dateString}' as dt
           , sum(v.c_sum_nt) as sum_nt
           from $Node2t_team_k7m_aux_d_crit_consult_inIN v
           left semi join $Node1t_team_k7m_aux_d_crit_consult_inIN t
           on case when v.ktdt = 0 then v.inn_st else v.inn_sec end = t.org_inn_crm_num
           --and t.od = d.date_cond_to
                where ((v.c_nazn like ${SparkMain.consultIn1}) or (v.c_nazn like ${SparkMain.consultIn2}))
                    and v.c_nazn like ${SparkMain.consultIn3}
                    and v.c_date_prov >= cast(add_months(date'${dateString}',-12) as string)
                    and v.c_date_prov < '${dateString}'
                    and v.inn_st  <> '${SparkMain.innSber}'
                    and v.inn_sec <> '${SparkMain.innSber}'
                    and filt = 0
          group by (case when v.ktdt = 0 then v.inn_sec else v.inn_st end), (case when v.ktdt = 0 then v.inn_st else v.inn_sec end)"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_consult_inPrep")



    val createHiveTableStage2 = spark.sql(
      s"""select
     sl.inn_dt as inn1
    ,sl.inn_kt as inn2
    ,sl.dt
    ,sl.sum_nt
    ,(sl.sum_nt / re.revenue_L12M_true) as quantity
    ,re.revenue_L12M_true
  from $Nodet_team_k7m_aux_d_crit_consult_inPrep sl
  left join $Node3t_team_k7m_aux_d_crit_consult_inIN re
    on sl.inn_kt = re.inn
   --and sl.dt = re.dt"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_consult_inOUT")

    logInserted()
    logEnd()
  }

}
