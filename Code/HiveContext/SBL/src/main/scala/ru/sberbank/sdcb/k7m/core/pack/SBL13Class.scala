package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL13Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_adregrul_inIN = s"${config.aux}.basis_client"
  val Node2t_team_k7m_aux_d_crit_adregrul_inIN = s"${config.stg}.int_ul_organization_egrul" // требуется история
  val Nodet_team_k7m_aux_d_crit_adregrul_inOUT = s"${config.aux}.crit_adregrul"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_adregrul_inOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_adregrul"

  def DoSBL13CritAdrEgrul(dateString: String, dateStringIntegrum: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_adregrul_inOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_crit_adregrul_inPrep = Nodet_team_k7m_aux_d_crit_adregrul_inOUT.concat    ("_Prep")
    val Nodet_team_k7m_aux_d_crit_adregrul_inAdr = Nodet_team_k7m_aux_d_crit_adregrul_inOUT.concat     ("_Adr")
    val Nodet_team_k7m_aux_d_crit_adregrul_inAdrCount = Nodet_team_k7m_aux_d_crit_adregrul_inOUT.concat("_AdrCount")
    val Nodet_team_k7m_aux_d_crit_adregrul_inAdrBig = Nodet_team_k7m_aux_d_crit_adregrul_inOUT.concat  ("_AdrBig")

    val createHiveTableStage1 = spark.sql(
      s"""select ul_inn,
            lower(ul_adrs_rf_subject) as ul_adrs_rf_subject,
            lower(ul_adrs_street) as ul_adrs_street,
            lower(ul_adrs_city) as ul_adrs_city,
            lower(ul_adrs_house_num) as ul_adrs_house_num,
            lower(ul_adrs_full) as ul_adrs_full,
            '${dateString}' as dt
        from (select *, row_number() over (partition by ul_org_id order by effectivefrom desc) rn
        from $Node2t_team_k7m_aux_d_crit_adregrul_inIN o
       where effectivefrom < '${dateString}'
         and effectiveto >= '${dateString}'
          --o.effectiveto = '${dateStringIntegrum}'
          ) o
        where o.rn =1 and nvl(o.ul_activity_stop_method_nm,'') = ''"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Adr").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adregrul_inAdr")


    val createHiveTableStage2 = spark.sql(
      s"""select 
      dt, 
      ul_adrs_rf_subject, 
      ul_adrs_street, 
      ul_adrs_city, 
      ul_adrs_house_num, 
      ul_adrs_full, 
      count(distinct ul_inn) as adr_cnt
  from $Nodet_team_k7m_aux_d_crit_adregrul_inAdr
 group by dt, 
          ul_adrs_rf_subject, 
          ul_adrs_street, 
          ul_adrs_city, 
          ul_adrs_house_num, 
          ul_adrs_full"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_AdrCount").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adregrul_inAdrCount")

    val createHiveTableStage3 = spark.sql(
      s"""select
       egrul.ul_inn  as inn1,
       egrul2.ul_inn  as inn2,
       egrul.dt as dt,
       max(adrcount.adr_cnt) as quantity
  from $Nodet_team_k7m_aux_d_crit_adregrul_inAdr egrul
  join $Nodet_team_k7m_aux_d_crit_adregrul_inAdr egrul2
    on egrul.ul_adrs_rf_subject = egrul2.ul_adrs_rf_subject
   and egrul.ul_adrs_street = egrul2.ul_adrs_street
   and egrul.ul_adrs_city = egrul2.ul_adrs_city
   and egrul.ul_adrs_house_num = egrul2.ul_adrs_house_num
   and egrul.ul_adrs_full = egrul2.ul_adrs_full
  left join $Nodet_team_k7m_aux_d_crit_adregrul_inAdrCount adrcount
    on egrul.ul_adrs_rf_subject = adrcount.ul_adrs_rf_subject
   and egrul.ul_adrs_street = adrcount.ul_adrs_street
   and egrul.ul_adrs_city = adrcount.ul_adrs_city
   and egrul.ul_adrs_house_num = adrcount.ul_adrs_house_num
   and egrul.ul_adrs_full = adrcount.ul_adrs_full
 where  egrul.ul_inn <> egrul2.ul_inn
 group by egrul.ul_inn, egrul2.ul_inn, egrul.dt"""
        ).write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .option("path", s"${dashboardPath}_AdrBig").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adregrul_inAdrBig")


    val createHiveTableStage4 = spark.sql(
      s"""select
      egrul.inn1
      ,egrul.inn2
      ,egrul.dt
      ,egrul.quantity
  from $Node1t_team_k7m_aux_d_crit_adregrul_inIN b
  join $Nodet_team_k7m_aux_d_crit_adregrul_inAdrBig egrul
    on b.org_inn_crm_num = egrul.inn1
   --and b.od = egrul.dt
   union all
select
      egrul.inn1
      ,egrul.inn2
      ,egrul.dt
      ,egrul.quantity
  from $Node1t_team_k7m_aux_d_crit_adregrul_inIN b
  join $Nodet_team_k7m_aux_d_crit_adregrul_inAdrBig egrul
    on b.org_inn_crm_num = egrul.inn2
   --and b.od = egrul.dt """
        ).write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .option("path", s"${dashboardPath}_Prep").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adregrul_inPrep")



    val createHiveTableStage5 = spark.sql(
      s"""select
      inn1, 
      inn2, 
      dt, 
      max(quantity) as quantity
  from $Nodet_team_k7m_aux_d_crit_adregrul_inPrep
 where inn1 <> '${SparkMain.innSber}'
   and inn2 <> '${SparkMain.innSber}'
 group by inn1, inn2, dt"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_adregrul_inOUT")

    logInserted()
    logEnd()
  }

}
