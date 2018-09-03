package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CRMRepOrgstructuresClass(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa
  val Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN = s"${DevSchema}.K7M_CRM_org_base"
  val Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT = s"${DevSchema}.K7M_CRM_rep_orgstructures"
  val dashboardPath = s"${config.auxPath}K7M_CRM_rep_orgstructures"


  var dashboardName: String = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT //витрина
  override def processName: String = "CRM_BASE"

  def DoCRMRepOrgStruct() {

    val Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_1_ca = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT.concat("_1_ca")
    val Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_2_tb = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT.concat("_2_tb")
    val Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_3_dep = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT.concat("_3_dep")
    val Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_4_gosb = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT.concat("_4_gosb")
    val Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_5_osb = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT.concat("_5_osb")
    val Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_6_upr = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT.concat("_6_upr")
    val Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_7_otd = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT.concat("_7_otd")
    val Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_8_vsp = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT.concat("_8_vsp")
    val Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_9_sector = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT.concat("_9_sector")

    Logger.getLogger(Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT).setLevel(Level.WARN)

    logStart()

    dashboardName = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_1_ca
    val createHiveTableStage2 = spark.sql(
      s"""select
      id,
      concat('/',o.lvl) lvl_path,
      1 leaf,
      name,
      business_area,
      id as ca_id,
      name as ca_name,
      code as ca_code,
      created_dt
  from $Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN o
 where nvl(o.parent_id,'') =''
   and o.lvl = 'ЦА'""").write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_1_ca"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_1_ca")

    logInserted()
    dashboardName = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_2_tb

    val createHiveTableStage3 = spark.sql(
      s"""select
      r.id,
      r.lvl_path,
      r.leaf,
      r.name,
      r.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      cast(null as string) tb_id,
      cast(null as string) tb_name,
      cast(null as string) tb_code,
      r.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_1_ca r"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_2_tb"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_2_tb")


    val createHiveTableStage4 = spark.sql(
      s"""select
      o.id,
      concat( r.lvl_path,'/',o.lvl) lvl_path,
      1 leaf,
      o.name,
      o.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      o.id as tb_id,
      o.name as tb_name,
      o.code as tb_code,
      o.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_1_ca r
  join $Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN o
    on r.id = o.parent_id
   and o.lvl = 'ТБ'"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath.concat("_2_tb"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_2_tb")

    logInserted()
    dashboardName = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_3_dep

    val createHiveTableStage5 = spark.sql(
      s"""select
      r.id,
      r.lvl_path,
      r.leaf,
      r.name,
      r.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      cast(null as string) dep_id,
      cast(null as string) dep_name,
      cast(null as string) dep_code,
      r.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_2_tb r"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_3_dep"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_3_dep")


    val createHiveTableStage6 = spark.sql(
      s"""select
      o.id,
      concat( r.lvl_path,'/',o.lvl) lvl_path,
      1 leaf,
      o.name,
      o.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      o.id as dep_id,
      o.name as dep_name,
      o.code as dep_code,
      o.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_2_tb r
  join $Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN o
    on r.id = o.parent_id
   and o.lvl = 'Департамент'"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath.concat("_3_dep"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_3_dep")

    logInserted()
    dashboardName = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_4_gosb

    val createHiveTableStage7 = spark.sql(
      s"""select
      r.id,
      r.lvl_path,
      r.leaf,
      r.name,
      r.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      cast(null as string) gosb_id,
      cast(null as string) gosb_name,
      cast(null as string) gosb_code,
      r.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_3_dep r"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_4_gosb"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_4_gosb")


    val createHiveTableStage8 = spark.sql(
      s"""select
      o.id,
      concat( r.lvl_path,'/',o.lvl) lvl_path,
      1 leaf,
      o.name,
      o.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      o.id as gosb_id,
      o.name as gosb_name,
      o.code as gosb_code,
      o.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_3_dep r
  join $Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN o
    on r.id = o.parent_id
   and o.lvl = 'ГОСБ'"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath.concat("_4_gosb"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_4_gosb")

    logInserted()
    dashboardName = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_5_osb

    val createHiveTableStage9 = spark.sql(
      s"""select
      r.id,
      r.lvl_path,
      r.leaf,
      r.name,
      r.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      r.gosb_id,
      r.gosb_name,
      r.gosb_code,
      cast(null as string) osb_id,
      cast(null as string) osb_name,
      cast(null as string) osb_code,
      r.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_4_gosb r"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_5_osb"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_5_osb")


    val createHiveTableStage10 = spark.sql(
      s"""select
      o.id,
      concat( r.lvl_path,'/',o.lvl) lvl_path,
      1 leaf,
      o.name,
      o.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      r.gosb_id,
      r.gosb_name,
      r.gosb_code,
      o.id as osb_id,
      o.name as osb_name,
      o.code as osb_code,
      o.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_4_gosb r
  join $Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN o
    on r.id = o.parent_id
   and o.lvl = 'ОСБ'"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath.concat("_5_osb"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_5_osb")

    logInserted()
    dashboardName = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_6_upr

    val createHiveTableStage11 = spark.sql(
      s"""select
      r.id,
      r.lvl_path,
      r.leaf,
      r.name,
      r.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      r.gosb_id,
      r.gosb_name,
      r.gosb_code,
      r.osb_id,
      r.osb_name,
      r.osb_code,
      cast(null as string) upr_id,
      cast(null as string) upr_name,
      cast(null as string) upr_code,
      r.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_5_osb r"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_6_upr"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_6_upr")


    val createHiveTableStage12 = spark.sql(
      s"""select
      o.id,
      concat( r.lvl_path,'/',o.lvl) lvl_path,
      1 leaf,
      o.name,
      o.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      r.gosb_id,
      r.gosb_name,
      r.gosb_code,
      r.osb_id,
      r.osb_name,
      r.osb_code,
      o.id as upr_id,
      o.name as upr_name,
      o.code as upr_code,
      o.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_5_osb r
  join $Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN o
    on r.id = o.parent_id
   and o.lvl = 'Управление'"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath.concat("_6_upr"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_6_upr")

    logInserted()
    dashboardName = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_7_otd

    val createHiveTableStage13 = spark.sql(
      s"""select
      r.id,
      r.lvl_path,
      r.leaf,
      r.name,
      r.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      r.gosb_id,
      r.gosb_name,
      r.gosb_code,
      r.osb_id,
      r.osb_name,
      r.osb_code,
      r.upr_id,
      r.upr_name,
      r.upr_code,
      cast(null as string) otdel_id,
      cast(null as string) otdel_name,
      cast(null as string) otdel_code,
      r.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_6_upr r"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_7_otd"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_7_otd")


    val createHiveTableStage14 = spark.sql(
      s"""select
      o.id,
      concat( r.lvl_path,'/',o.lvl) lvl_path,
      1 leaf,
      o.name,
      o.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      r.gosb_id,
      r.gosb_name,
      r.gosb_code,
      r.osb_id,
      r.osb_name,
      r.osb_code,
      r.upr_id,
      r.upr_name,
      r.upr_code,
      o.id as otdel_id,
      o.name as otdel_name,
      o.code as otdel_code,
      o.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_6_upr r
  join $Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN o
    on r.id = o.parent_id
   and o.lvl = 'Отдел'"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath.concat("_7_otd"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_7_otd")

    logInserted()
    dashboardName = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_8_vsp

    val createHiveTableStage15 = spark.sql(
      s"""select
      r.id,
      r.lvl_path,
      r.leaf,
      r.name,
      r.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      r.gosb_id,
      r.gosb_name,
      r.gosb_code,
      r.osb_id,
      r.osb_name,
      r.osb_code,
      r.upr_id,
      r.upr_name,
      r.upr_code,
      r.otdel_id,
      r.otdel_name,
      r.otdel_code,
      cast(null as string) vsp_id,
      cast(null as string) vsp_name,
      cast(null as string) vsp_code,
      r.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_7_otd r"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_8_vsp"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_8_vsp")


    val createHiveTableStage16 = spark.sql(
      s"""select
      o.id,
      concat( r.lvl_path,'/',o.lvl) lvl_path,
      1 leaf,
      o.name,
      o.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      r.gosb_id,
      r.gosb_name,
      r.gosb_code,
      r.osb_id,
      r.osb_name,
      r.osb_code,
      r.upr_id,
      r.upr_name,
      r.upr_code,
      r.otdel_id,
      r.otdel_name,
      r.otdel_code,
      o.id as vsp_id,
      o.name as vsp_name,
      o.code as vsp_code,
      o.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_7_otd r
  join $Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN o
    on r.id = o.parent_id
   and o.lvl = 'ВСП'"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath.concat("_8_vsp"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_8_vsp")

    logInserted()
    dashboardName = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_9_sector

    val createHiveTableStage17 = spark.sql(
      s"""select
      r.id,
      r.lvl_path,
      r.leaf,
      r.name,
      r.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      r.gosb_id,
      r.gosb_name,
      r.gosb_code,
      r.osb_id,
      r.osb_name,
      r.osb_code,
      r.upr_id,
      r.upr_name,
      r.upr_code,
      r.otdel_id,
      r.otdel_name,
      r.otdel_code,
      r.vsp_id,
      r.vsp_name,
      r.vsp_code,
      cast(null as string) sector_id,
      cast(null as string) sector_name,
      cast(null as string) sector_code,
      r.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_8_vsp r"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath.concat("_9_sector"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_9_sector")


    val createHiveTableStage18 = spark.sql(
      s"""select
      o.id,
      concat( r.lvl_path,'/',o.lvl) lvl_path,
      1 leaf,
      o.name,
      o.business_area,
      r.ca_id,
      r.ca_name,
      r.ca_code,
      r.tb_id,
      r.tb_name,
      r.tb_code,
      r.dep_id,
      r.dep_name,
      r.dep_code,
      r.gosb_id,
      r.gosb_name,
      r.gosb_code,
      r.osb_id,
      r.osb_name,
      r.osb_code,
      r.upr_id,
      r.upr_name,
      r.upr_code,
      r.otdel_id,
      r.otdel_name,
      r.otdel_code,
      r.vsp_id,
      r.vsp_name,
      r.vsp_code,
      o.id as sector_id,
      o.name as sector_name,
      o.code as sector_code,
      o.created_dt
  from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_8_vsp r
  join $Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN o
    on r.id = o.parent_id
   and o.lvl = 'Сектор'"""
    ).write
      .format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath.concat("_9_sector"))
      .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_9_sector")

    logInserted()
    dashboardName = Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT


    val createHiveTableStage19 = spark.sql(
      s"""select
    id,
    lvl_path,
    case when coalesce(cc.child_cnt,0) > 0 then 0 else 1 end leaf,
    name,
    business_area,
    ca_id,
    ca_name,
    ca_code,
    tb_id,
    tb_name,
    tb_code,
    dep_id,
    dep_name,
    dep_code,
    gosb_id,
    gosb_name,
    gosb_code,
    osb_id,
    osb_name,
    osb_code,
    upr_id,
    upr_name,
    upr_code,
    otdel_id,
    otdel_name,
    otdel_code,
    vsp_id,
    vsp_name,
    vsp_code,
    sector_id,
    sector_name,
    sector_code,
    created_dt
    from $Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructures_9_sector stg
      left join (select parent_id, count(*) child_cnt
      from $Node1t_team_k7m_aux_d_K7M_CRM_rep_orgstructuresIN
    group by parent_id) cc
    on stg.id = cc.parent_id"""
    ).write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .option("path", dashboardPath)
    .saveAsTable(s"$Nodet_team_k7m_aux_d_K7M_CRM_rep_orgstructuresOUT")

    logInserted()
    logEnd()
  }
}
