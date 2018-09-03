package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL10Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_sendloansIN = s"${config.aux}.basis_client"
  val Node2t_team_k7m_aux_d_crit_sendloansIN = s"${config.aux}.trbasis_kras"
  val Node3t_team_k7m_aux_d_crit_sendloansIN = s"${config.aux}.Revenue_Target"
  val Node4t_team_k7m_aux_d_crit_sendloansIN = s"${config.aux}.int_ip_organization_egrip_sbl"
  val Nodet_team_k7m_aux_d_crit_sendloansOUT = s"${config.aux}.crit_sendloans"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_sendloansOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_sendloans"

  def DoSBL10CritSendLoans(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_sendloansOUT).setLevel(Level.WARN)

    logStart()
   import SparkMain._
    val Nodet_team_k7m_aux_d_crit_sendloansPrep = Nodet_team_k7m_aux_d_crit_sendloansOUT.concat("_Prep")

    val createHiveTableStage1 = spark.sql(
      s"""select
           case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn_dt
           , case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn_kt
           , '${dateString}' as dt
           , sum(v.c_sum_nt) as sum_out_loans,
           sum(sum(v.c_sum_nt)) over (partition by case when v.ktdt = 0 then v.inn_sec else v.inn_st end) sum_out_loans_total
           from $Node2t_team_k7m_aux_d_crit_sendloansIN v
           left semi join $Node1t_team_k7m_aux_d_crit_sendloansIN t
           on case when v.ktdt = 0 then v.inn_sec else v.inn_st end = t.org_inn_crm_num
           --and t.od = d.date_cond_to
                where lower(v.predicted_value) in (${receiveLoans})
                    and v.c_date_prov >= cast(add_months(date'${dateString}',-12) as string)
                    and v.c_date_prov < '${dateString}'
                    and v.inn_st  <> '${innSber}'
                    and v.inn_sec <> '${innSber}'
                    and filt = 0
         and   (
             (length(case when v.ktdt = 0 then v.inn_st else v.inn_sec end) = 10) or
             ((length(case when v.ktdt = 0 then v.inn_st else v.inn_sec end) = 12)
                   and (case when v.ktdt = 0 then v.inn_st else v.inn_sec end in (select ip_inn from $Node4t_team_k7m_aux_d_crit_sendloansIN)))
             )
          group by (case when v.ktdt = 0 then v.inn_sec else v.inn_st end), (case when v.ktdt = 0 then v.inn_st else v.inn_sec end)"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_sendloansPrep")


    val createHiveTableStage2 = spark.sql(
      s"""select
     sl.inn_dt as inn1
     ,sl.inn_kt as inn2
     ,sl.dt
     ,sl.sum_out_loans
    ,(sl.sum_out_loans / re.revenue_L12M_true) as quantity
    ,re.revenue_L12M_true
  from $Nodet_team_k7m_aux_d_crit_sendloansPrep sl
  left join $Node3t_team_k7m_aux_d_crit_sendloansIN re
    on sl.inn_dt = re.inn
 where sl.sum_out_loans / sl.sum_out_loans_total >= $CONST_DOWN_LOANS_SHARE """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_sendloansOUT")

    logInserted()
    logEnd()
  }

}