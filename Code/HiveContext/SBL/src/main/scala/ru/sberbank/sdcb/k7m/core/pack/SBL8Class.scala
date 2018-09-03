package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL8Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_payoffloans_outIN = s"${config.aux}.basis_client"
  val Node2t_team_k7m_aux_d_crit_payoffloans_outIN = s"${config.aux}.trbasis_kras"
  val Node3t_team_k7m_aux_d_crit_payoffloans_outIN = s"${config.aux}.Revenue_Target"
  val Node4t_team_k7m_aux_d_crit_payoffloans_outIN = s"${config.aux}.int_ip_organization_egrip_sbl"
  val Nodet_team_k7m_aux_d_crit_payoffloans_outOUT = s"${config.aux}.crit_payoffloans_out"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_payoffloans_outOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_payoffloans_out"

  def DoSBL8CritPayoffloansOut(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_payoffloans_outOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_crit_payoffloans_outPrep = Nodet_team_k7m_aux_d_crit_payoffloans_outOUT.concat("_Prep")
    val Nodet_team_k7m_aux_d_crit_payoffloans_outPrep2 = Nodet_team_k7m_aux_d_crit_payoffloans_outOUT.concat("_Prep2")

    val createHiveTableStage1 = spark.sql(
      s"""select
       case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn_dt
       ,case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn_kt
       ,'${dateString}' as dt
       ,sum(v.c_sum_nt) as sum_out_loans
  from $Node2t_team_k7m_aux_d_crit_payoffloans_outIN v
  left semi join $Node1t_team_k7m_aux_d_crit_payoffloans_outIN t
    on case when v.ktdt = 0 then v.inn_sec else v.inn_st end = t.org_inn_crm_num
 where v.predicted_value in (${SparkMain.payOffLoans})
   and v.c_date_prov >= cast(add_months(date'${dateString}',-12) as string)
   and v.c_date_prov < '${dateString}'
   and v.inn_st  <> '${SparkMain.innSber}'
   and v.inn_sec <> '${SparkMain.innSber}'
   and filt = 0
 group by (case when v.ktdt = 0 then v.inn_sec else v.inn_st end), (case when v.ktdt = 0 then v.inn_st else v.inn_sec end)
        """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_payoffloans_outPrep")

    val createHiveTableStage2 = spark.sql( s"""
    select  p.*
    from $Nodet_team_k7m_aux_d_crit_payoffloans_outPrep p
    where length(inn_kt) = 10
    union all
    select  p.*
    from $Nodet_team_k7m_aux_d_crit_payoffloans_outPrep p
    left semi join $Node4t_team_k7m_aux_d_crit_payoffloans_outIN a
    on (length(inn_kt) = 12 and inn_kt = a.ip_inn)
    """).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep2").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_payoffloans_outPrep2")

    val createHiveTableStage3 = spark.sql(
      s"""select distinct
        sl.inn_dt as inn1
    ,sl.inn_kt as inn2
    ,sl.dt
    ,sl.sum_out_loans
    ,(sl.sum_out_loans / re.revenue_L12M_true) as quantity
    ,re.revenue_L12M_true
  from $Nodet_team_k7m_aux_d_crit_payoffloans_outPrep2 sl
  left join $Node3t_team_k7m_aux_d_crit_payoffloans_outIN re
    on sl.inn_dt = re.inn
   --and sl.dt = re.dt"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_payoffloans_outOUT")

    logInserted()
    logEnd()
  }


}
