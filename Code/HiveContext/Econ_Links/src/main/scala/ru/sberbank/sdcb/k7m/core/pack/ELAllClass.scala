package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ELAllClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchemaCynomys = config.aux
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa


  val Node1t_team_k7m_aux_d_k7m_ELIN = s"${DevSchema}.k7m_EL_5211"
  val Node2t_team_k7m_aux_d_k7m_ELIN = s"${DevSchema}.k7m_EL_5212_5214"
  val Node3t_team_k7m_aux_d_k7m_ELIN = s"${DevSchema}.k7m_EL_5213"
  val Node4t_team_k7m_aux_d_k7m_ELIN = s"${DevSchema}.k7m_EL_5222"
  val Node5t_team_k7m_aux_d_k7m_ELIN = s"${Stg0Schema}.int_ul_organization_rosstat"
  val Node6t_team_k7m_aux_d_k7m_ELIN = s"${Stg0Schema}.int_org_okved_rosstat"
  val Nodet_team_k7m_aux_d_k7m_ELOUT = s"${DevSchema}.k7m_EL"
  val dashboardPath = s"${config.auxPath}k7m_EL"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_ELOUT //витрина
  override def processName: String = "EL"

  def DoELAll() {

    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_ELOUT).setLevel(Level.WARN)

    logStart()


    val Nodet_team_k7m_aux_d_k7m_ELPREP1 = Nodet_team_k7m_aux_d_k7m_ELOUT.concat("_Prep1")
    val Nodet_team_k7m_aux_d_k7m_ELPREP2 = Nodet_team_k7m_aux_d_k7m_ELOUT.concat("_Prep2")

    val createHiveTableStage1 = spark.sql(
      s"""
    select distinct ul_inn
    from (
      select ul_inn from $Node5t_team_k7m_aux_d_k7m_ELIN
    where ul_okopf_cd in ('2.02.02',--Профсоюзы
    '7.16.01','7.51.04')
    union all
      select org.ul_inn
    from $Node5t_team_k7m_aux_d_k7m_ELIN org
      join $Node6t_team_k7m_aux_d_k7m_ELIN rst
      on org.ul_org_id = rst.ul_org_id
    where rst.okvd_okved_cd like '70.32%'
       or rst.okvd_okved_cd like '68.32%') x""")
    .write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .option("path", s"${dashboardPath}_Prep1")
    .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_ELPREP1")

    val createHiveTableStage2 = spark.sql(
      s"""select distinct
    e.*
from (
    select * from $Node1t_team_k7m_aux_d_k7m_ELIN
    union all
    select * from $Node2t_team_k7m_aux_d_k7m_ELIN
    union all
    select * from $Node3t_team_k7m_aux_d_k7m_ELIN
    union all
    select * from $Node4t_team_k7m_aux_d_k7m_ELIN
    )    e
  where e.to_inn<>e.from_inn
    and length(e.from_inn) in (10,12)
    and length(e.to_inn)=10
    and  ${SparkMainClass.excludeProvKtInn} not in (e.from_inn,e.to_inn)
    and e.from_inn not like '00%'""")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep2")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_ELPREP2")

    val createHiveTableStage3 = spark.sql(
      s"""select row_number() over (order by crit_code, from_inn, to_inn) loc_id, *
          from ( select distinct e.*
    from $Nodet_team_k7m_aux_d_k7m_ELPREP2 e
    left join $Nodet_team_k7m_aux_d_k7m_ELPREP1 i
      on i.ul_inn = e.to_inn
    left join $Nodet_team_k7m_aux_d_k7m_ELPREP1 if
      on if.ul_inn = e.from_inn
   where i.ul_inn is null
     and if.ul_inn is null) a """)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_ELOUT")

    logInserted()
    logEnd()
  }
}
