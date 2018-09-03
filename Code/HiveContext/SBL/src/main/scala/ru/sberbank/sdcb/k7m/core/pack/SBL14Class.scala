package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL14Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_adrrosstatIN = s"${config.stg}.int_ul_organization_rosstat"
  val Node2t_team_k7m_aux_d_crit_adrrosstatIN = s"${config.stg}.int_financial_statement_rosstat"
  val Node3t_team_k7m_aux_d_crit_adrrosstatIN = s"${config.aux}.basis_client"
  val Nodet_team_k7m_aux_d_crit_adrrosstatOUT = s"${config.aux}.crit_adrrosstat"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_adrrosstatOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_adrrosstat"

  def DoSBL14CritAdrRosstat(dateString: String, dateStringIntegrum: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_adrrosstatOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_crit_adrrosstatPrep = Nodet_team_k7m_aux_d_crit_adrrosstatOUT.concat ("_Prep")
    val Nodet_team_k7m_aux_d_crit_adrrosstatUniq = Nodet_team_k7m_aux_d_crit_adrrosstatOUT.concat ("_Uniq")
    val Nodet_team_k7m_aux_d_crit_adrrosstatPrep1_5 = Nodet_team_k7m_aux_d_crit_adrrosstatOUT.concat("_Prep1_5")
    val Nodet_team_k7m_aux_d_crit_adrrosstatPrep2 = Nodet_team_k7m_aux_d_crit_adrrosstatOUT.concat("_Prep2")
    val Nodet_team_k7m_aux_d_crit_adrrosstatCount = Nodet_team_k7m_aux_d_crit_adrrosstatOUT.concat("_Count")
    val Nodet_team_k7m_aux_d_crit_adrrosstatBig = Nodet_team_k7m_aux_d_crit_adrrosstatOUT.concat  ("_Big")
    val Nodet_team_k7m_aux_d_crit_adrrosstatStat = Nodet_team_k7m_aux_d_crit_adrrosstatOUT.concat ("_Stat")

    val createHiveTableStage1 = spark.sql(
      s"""select distinct
     ul_inn
     ,lower(ul_adrs_full) as ul_adrs_full
     ,ul_org_id
     ,UL_CAPITAL_SUM
     ,ul_branch_cnt
    , '${dateString}' as dt
    from (select *, row_number() over (partition by ul_org_id order by effectivefrom desc) rn
  from $Node1t_team_k7m_aux_d_crit_adrrosstatIN a
 where --a.effectiveto = '${dateStringIntegrum}'
       a.effectiveto > '${dateString}'
   and a.effectivefrom <= '${dateString}') a
   where a.rn = 1
     and cast(a.UL_CAPITAL_SUM as double) > 0
     and a.UL_ACTIVE_FLG = 1
     and a.ul_inn is not null"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adrrosstatPrep")

    val createHiveTableStage2 = spark.sql(
      s"""select ul_inn
  from $Nodet_team_k7m_aux_d_crit_adrrosstatPrep
 group by ul_inn
having count(ul_adrs_full) = 1"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Uniq").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adrrosstatUniq")

    val createHiveTableStage2_5 = spark.sql(
      s"""   select ul_inn
                from $Nodet_team_k7m_aux_d_crit_adrrosstatPrep
               group by ul_inn, UL_CAPITAL_SUM
              having count(ul_org_id) > 1
             """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep1_5").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adrrosstatPrep1_5")

    val createHiveTableStage3 = spark.sql(
      s"""    select  c.ul_inn, c.adr as ul_adrs_full,  c.ul_org_id, c.UL_CAPITAL_SUM, c.ul_branch_cnt, c.dt
    from (select distinct
                a.ul_inn,
                a.dt,
                a.UL_CAPITAL_SUM,
                lower(a.ul_adrs_full) as adr,
                a.ul_org_id,
                a.ul_branch_cnt,
                row_number() over (partition by a.ul_org_id order by a.UL_CAPITAL_SUM desc) as rn
        from (select * from $Nodet_team_k7m_aux_d_crit_adrrosstatPrep t
                      where t.ul_branch_cnt is not null) a
        left semi join $Nodet_team_k7m_aux_d_crit_adrrosstatPrep1_5 b
          on a.ul_inn = b.ul_inn
        left semi join $Node2t_team_k7m_aux_d_crit_adrrosstatIN v
          on a.ul_org_id = v.ul_org_id
    ) c
    where c.rn = 1"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep2").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adrrosstatPrep2")

    val createHiveTableStage4 = spark.sql(
      s"""    select  a.ul_inn,  a.ul_adrs_full,  a.ul_org_id, a.UL_CAPITAL_SUM, a.ul_branch_cnt, a.dt
    from $Nodet_team_k7m_aux_d_crit_adrrosstatPrep a
    left semi join $Nodet_team_k7m_aux_d_crit_adrrosstatUniq b
    on a.ul_inn = b.ul_inn"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", s"${dashboardPath}_Prep2").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adrrosstatPrep2")

    val createHiveTableStage5 = spark.sql(
      s"""select dt, ul_adrs_full, count( ul_inn) as adr_cnt
from $Nodet_team_k7m_aux_d_crit_adrrosstatPrep
group by dt, ul_adrs_full"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Count").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adrrosstatCount")

    val createHiveTableStage6 = spark.sql(
      s"""select
      rosstat.ul_inn  as inn1,
      rosstat2.ul_inn  as inn2,
      rosstat.dt as dt,
      rosstat.ul_adrs_full,
      max(adrcount.adr_cnt) as quantity
  from $Nodet_team_k7m_aux_d_crit_adrrosstatPrep2 rosstat
  join $Nodet_team_k7m_aux_d_crit_adrrosstatPrep2 rosstat2
    on rosstat.ul_adrs_full = rosstat2.ul_adrs_full
  join $Nodet_team_k7m_aux_d_crit_adrrosstatCount adrcount
    on rosstat.ul_adrs_full = adrcount.ul_adrs_full
 where rosstat.ul_inn <> rosstat2.ul_inn
 group by rosstat.ul_inn, rosstat2.ul_inn, rosstat.dt, rosstat.ul_adrs_full"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Big").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adrrosstatBig")

    val createHiveTableStage7 = spark.sql(
      s"""select
                rosstat.inn1
                ,rosstat.inn2
                ,rosstat.dt
                ,rosstat.quantity
                ,rosstat.ul_adrs_full
    from $Nodet_team_k7m_aux_d_crit_adrrosstatBig rosstat
    left semi join $Node3t_team_k7m_aux_d_crit_adrrosstatIN b
    on b.org_inn_crm_num = rosstat.inn1"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Stat").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adrrosstatStat")

    val createHiveTableStage8 = spark.sql(
      s"""select
                rosstat.inn1
                ,rosstat.inn2
                ,rosstat.dt
                ,rosstat.quantity
                ,rosstat.ul_adrs_full
    from $Nodet_team_k7m_aux_d_crit_adrrosstatBig rosstat
    left semi join $Node3t_team_k7m_aux_d_crit_adrrosstatIN b
    on b.org_inn_crm_num = rosstat.inn2"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", s"${dashboardPath}_Stat").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adrrosstatStat")



    val createHiveTableStage9 = spark.sql(
      s"""select inn1, inn2, dt, ul_adrs_full, max(quantity) as quantity
  from $Nodet_team_k7m_aux_d_crit_adrrosstatStat
 where inn1 <> '${SparkMain.innSber}'
   and inn2 <> '${SparkMain.innSber}'
 group by inn1, inn2, dt, ul_adrs_full"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adrrosstatOUT")

    logInserted()
    logEnd()
  }
}