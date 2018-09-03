package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL4Class (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_headonloanIN = s"${config.aux}.trbasis_kras"
  val Node2t_team_k7m_aux_d_crit_headonloanIN = s"${config.aux}.basis_client"
   val Nodet_team_k7m_aux_d_crit_headonloanOUT = s"${config.aux}.crit_headonloan"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_headonloanOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_headonloan"

  def DoSBL4CritHeadOnLoan(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_headonloanOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_crit_headonloanTemp = Nodet_team_k7m_aux_d_crit_headonloanOUT.concat("_TempSendLoan")

    val createHiveTableStage1 = spark.sql(
      s"""    select
          case when v.ktdt = 0 then v.inn_sec else v.inn_st end as inn_dt
         ,case when v.ktdt = 0 then v.inn_st else v.inn_sec end as inn_kt
         ,'${dateString}' as dt
         ,id
        from $Node1t_team_k7m_aux_d_crit_headonloanIN v
        where v.predicted_value in ('выдача займа') and c_date_prov < '${dateString}'
                                          and ((case when v.ktdt = 0 then v.inn_sec else v.inn_st end in (select org_inn_crm_num from $Node2t_team_k7m_aux_d_crit_headonloanIN)) or
                                               (case when v.ktdt = 0 then v.inn_st else v.inn_sec end in (select org_inn_crm_num from $Node2t_team_k7m_aux_d_crit_headonloanIN)))
                        and v.inn_st  <> '${SparkMain.innSber}'
                        and v.inn_sec <> '${SparkMain.innSber}'
                        and (filt = 0)
        """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_TempSendLoan").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_headonloanTemp")



    val createHiveTableStage2 = spark.sql(
      s"""    select inn1, inn2, dt, count(id) as quantity
         from
         (select     a.inn_dt as inn1,
                     a.inn_kt as inn2,
                     a.dt,
                     b.id
             from  ( select distinct inn_dt, inn_kt, dt
                     from   $Nodet_team_k7m_aux_d_crit_headonloanTemp) a
             inner join $Nodet_team_k7m_aux_d_crit_headonloanTemp b
             on ((a.inn_dt = b.inn_kt) and (a.inn_kt = b.inn_dt) and (a.dt = b.dt))
         )
         group by inn1, inn2, dt
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_headonloanOUT")

   logEnd()
  }
}

