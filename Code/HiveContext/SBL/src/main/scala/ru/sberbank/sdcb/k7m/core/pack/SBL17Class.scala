package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL17Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_foundersIN = s"${config.stg}.int_ul_organization_egrul"
  val Node2t_team_k7m_aux_d_crit_foundersIN = s"${config.stg}.int_org_founder_egrul"
  val Node3t_team_k7m_aux_d_crit_foundersIN = s"${config.aux}.basis_client"
  val Nodet_team_k7m_aux_d_crit_foundersOUT = s"${config.aux}.crit_founders"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_foundersOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_founders"

  def DoSBL17CritFounders50(dateString: String,dateStringIntegrum: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_foundersOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_crit_foundersOrg = Nodet_team_k7m_aux_d_crit_foundersOUT.concat("_Org")
    val Nodet_team_k7m_aux_d_crit_foundersFounders = Nodet_team_k7m_aux_d_crit_foundersOUT.concat("_Founders")
    val Nodet_team_k7m_aux_d_crit_foundersFoundCount = Nodet_team_k7m_aux_d_crit_foundersOUT.concat("_FoundCount")
    val Nodet_team_k7m_aux_d_crit_foundersFoundLKByINN = Nodet_team_k7m_aux_d_crit_foundersOUT.concat("_FoundLKByINN")
    val Nodet_team_k7m_aux_d_crit_foundersFoundInterceptCount = Nodet_team_k7m_aux_d_crit_foundersOUT.concat("_FoundInterceptCount")
    val Nodet_team_k7m_aux_d_crit_foundersFoundLKShare = Nodet_team_k7m_aux_d_crit_foundersOUT.concat("_FoundLKShare")
    val Nodet_team_k7m_aux_d_crit_foundersPrep = Nodet_team_k7m_aux_d_crit_foundersOUT.concat("_Prep")

    val createHiveTableStage1 = spark.sql(
      s"""select
      ul_inn,
      egrul_org_id,
      '${dateString}' as dt
  from (select *, row_number() over (partition by ul_org_id order by effectivefrom desc) as rn
          from $Node1t_team_k7m_aux_d_crit_foundersIN
         where effectivefrom <= '${dateString}'
           and effectiveto > '${dateString}') a
 where a.rn = 1
   and nvl(a.ul_activity_stop_method_nm,'') = ''
   and a.ul_inn <> '${SparkMain.innSber}'
   and nvl(a.ul_inn,'') <> ''"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Org").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_foundersOrg")



    val createHiveTableStage2 = spark.sql(
      s"""select
      aa.egrul_org_id_established
      ,aa.of_inn
      , aa.of_nm
      , b.ul_inn
      ,'${dateString}' as dt
      , row_number() over (partition by aa.of_inn, b.ul_inn order by aa.of_actual_dt desc) as rn
  from (select * from  (select row_number() over (partition by f.of_nm, f.egrul_org_id_established order by f.of_actual_dt  desc) as rn,
               f.*
          from $Node2t_team_k7m_aux_d_crit_foundersIN f
         where of_actual_dt < '${dateString}'
           and effectivefrom <= '${dateString}'
           and effectiveto > '${dateString}'
        ) a where a.rn = 1 and a.of_type = 1 and length(a.of_inn) <> 10) aa
  join $Nodet_team_k7m_aux_d_crit_foundersOrg b
   on
        aa.egrul_org_id_established = b.egrul_org_id
   """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Founders").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_foundersFounders")

    val createHiveTableStage3 = spark.sql(
      s"""select ul_inn, dt, count(distinct of_nm) as cnt
        from $Nodet_team_k7m_aux_d_crit_foundersFounders
        group by ul_inn, dt"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_FoundCount").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_foundersFoundCount")

    val createHiveTableStage4 = spark.sql(
      s"""select distinct
      left_egrul.egrul_org_id_established as left_egrul_id,
      right_egrul.egrul_org_id_established as right_egrul_id,
      left_egrul.ul_inn as left_inn,
      right_egrul.ul_inn as right_inn,
      left_egrul.dt,
      left_egrul.of_inn as of_inn
  from $Nodet_team_k7m_aux_d_crit_foundersFounders left_egrul
  join $Nodet_team_k7m_aux_d_crit_foundersFounders right_egrul
    on left_egrul.of_inn = right_egrul.of_inn
   and lower(left_egrul.of_nm) = lower(right_egrul.of_nm)
   and left_egrul.egrul_org_id_established <> right_egrul.egrul_org_id_established
   and left_egrul.ul_inn <> right_egrul.ul_inn
   --and left_egrul.dt = right_egrul.dt
        """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_FoundLKByINN").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_foundersFoundLKByINN")

    val createHiveTableStage5 = spark.sql(
      s"""select left_inn, right_inn, dt, count(distinct of_inn) as intercept_founders_count
  from $Nodet_team_k7m_aux_d_crit_foundersFoundLKByINN
 group by left_inn, right_inn, dt"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_FoundInterceptCount").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_foundersFoundInterceptCount")

    val createHiveTableStage6 = spark.sql(
      s"""select
      intc.left_inn
      ,left_egrul.cnt as left_founders_count
      ,(intc.intercept_founders_count / left_egrul.cnt ) as left_share
      ,intc.right_inn
      ,right_egrul.cnt as right_founders_count
      ,( intc.intercept_founders_count / right_egrul.cnt) as right_share
      ,intc.intercept_founders_count
      ,left_egrul.dt
  from $Nodet_team_k7m_aux_d_crit_foundersFoundInterceptCount intc
  left join $Nodet_team_k7m_aux_d_crit_foundersFoundCount left_egrul
    on intc.left_inn = left_egrul.ul_inn
  left join $Nodet_team_k7m_aux_d_crit_foundersFoundCount right_egrul
    on intc.right_inn = right_egrul.ul_inn"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_FoundLKShare").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_foundersFoundLKShare")

    val createHiveTableStage7 = spark.sql(
      s"""select  distinct
  --    f.left_egrul_id,
        left_inn as inn1,
        f.left_founders_count,
        f.left_share as quantity,
--      f.right_egrul_id,
        right_inn as inn2,
        f.right_founders_count,
        f.right_share,
        f.intercept_founders_count,
        f.dt
  from $Nodet_team_k7m_aux_d_crit_foundersFoundLKShare f
          --    where f.left_share >= 0.5 and f.right_share >= 0.5
    --   and left_egrul.ul_inn <> '' and right_egrul.ul_inn <> ''
    --   and left_egrul.ul_inn is not null and right_egrul.ul_inn is not null"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_foundersPrep")


    val createHiveTableStage8 = spark.sql(
      s"""select inn1, inn2, dt, max(quantity) as quantity
      from
      (select b.inn1, b.inn2, b.dt, b.quantity
        from $Node3t_team_k7m_aux_d_crit_foundersIN a
        join $Nodet_team_k7m_aux_d_crit_foundersPrep b
          on a.org_inn_crm_num = b.inn1
       union all
      select d.inn1, d.inn2, d.dt, d.quantity
        from $Node3t_team_k7m_aux_d_crit_foundersIN c
        join $Nodet_team_k7m_aux_d_crit_foundersPrep d
        on c.org_inn_crm_num = d.inn2
        )
    where inn1 <> '' and inn2 <> '' and inn1 is not null and inn2 is not null
        group by  inn1, inn2, dt"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_foundersOUT")

    logInserted()
    logEnd()
  }
}