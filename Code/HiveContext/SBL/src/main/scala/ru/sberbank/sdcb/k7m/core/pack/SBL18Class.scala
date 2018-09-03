package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._



class SBL18Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  import spark.implicits._

  val Node1t_team_k7m_aux_d_crit_crmlkIN = s"${config.stg}.CRM_cx_gsz"
  val Node2t_team_k7m_aux_d_crit_crmlkIN = s"${config.stg}.CRM_CX_PARTY_GSZ_X"
  val Node3t_team_k7m_aux_d_crit_crmlkIN = s"${config.stg}.CRM_S_PARTY"
  val Node4t_team_k7m_aux_d_crit_crmlkIN = s"${config.stg}.CRM_S_ORG_EXT"
  val Node5t_team_k7m_aux_d_crit_crmlkIN = s"${config.stg}.CRM_S_POSTN"
  val Node6t_team_k7m_aux_d_crit_crmlkIN = s"${config.stg}.CRM_CX_GSZMEM_INTER"
  val Node7t_team_k7m_aux_d_crit_crmlkIN = s"${config.stg}.CRM_CX_GSZ_CRITER"
  val Node8t_team_k7m_aux_d_crit_crmlkIN = s"${config.stg}.CRM_S_ORG_EXT_X"
  val Node9t_team_k7m_aux_d_crit_crmlkIN = s"${config.stg}.CRM_cx_gsz_crit_adm"
  val Node10t_team_k7m_aux_d_crit_crmlkIN = s"${config.aux}.basis_client"
  val Nodet_team_k7m_aux_d_crit_crmlkOUT = s"${config.aux}.crit_crmlk"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_crmlkOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_crmlk"

  def DoSBL18CritCRMLK() {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_crmlkOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_crit_crmlkPrep = Nodet_team_k7m_aux_d_crit_crmlkOUT.concat("_Prep")

//    val crmlkStage1 = spark.table(Node1t_team_k7m_aux_d_crit_crmlkIN).as("gsz")
//    val crmlkStage2 = crmlkStage1.join(spark.table(Node2t_team_k7m_aux_d_crit_crmlkIN).as("l_mbr"),$"l_mbr.gsz_id" === $"gsz.row_id","left")
//    val crmlkStage3 = crmlkStage2.join(spark.table(Node3t_team_k7m_aux_d_crit_crmlkIN).as("mbr"),$"mbr.row_id" === $"l_mbr.row_id","left")
//    val crmlkStage4 = crmlkStage3.join(spark.table(Node4t_team_k7m_aux_d_crit_crmlkIN).as("ext"),$"ext.row_id" === $"mbr.row_id" && $"mbr.PARTY_TYPE_CD" === lit(SparkMain.partyTypeOrg),"left")
//    val crmlkStage5 = crmlkStage4.join(spark.table(Node5t_team_k7m_aux_d_crit_crmlkIN).as("pos1"),$"pos1.row_id" === $"ext.pr_postn_id","left")
//    val crmlkStage6 = crmlkStage5.join(spark.table(Node6t_team_k7m_aux_d_crit_crmlkIN).as("i"),$"i.PARTY_ID" === $"mbr.ROW_ID","left")
//    val crmlkStage7 = crmlkStage6.join(spark.table(Node6t_team_k7m_aux_d_crit_crmlkIN).as("i2"), $"i2.LINK_ID" === $"i.LINK_ID" && $"i.PARTY_ID" =!= $"i2.PARTY_ID","left")
//    val crmlkStage8 = crmlkStage7.join(spark.table(Node3t_team_k7m_aux_d_crit_crmlkIN).as("mbr2"), $"(mbr2.row_id" === $"i2.PARTY_ID" ,"left")

    val createHiveTableStage1 = spark.sql(
      s"""    select
            i.ROW_ID as lk_id
            ,I3.CRIT_ID
            ,adm.name
            ,mbr2.row_id as CRM_ID_FROM
            ,ext_x1.sbrf_inn as inn1
            ,ext_x1.sbrf_kio as kio1
            ,mbr.row_id as id2
            ,mbr.name as name2
            ,ext_x2.sbrf_inn as inn2
            ,ext_x2.sbrf_kio as kio2
            ,gsz.row_id as gsz_id
            ,gsz.name as gsz_name
            ,mbr.row_id as CRM_ID_TO
            ,1 as quantity
        from $Node1t_team_k7m_aux_d_crit_crmlkIN gsz
   left join $Node2t_team_k7m_aux_d_crit_crmlkIN l_mbr on (l_mbr.gsz_id = gsz.row_id)
   left join $Node3t_team_k7m_aux_d_crit_crmlkIN mbr on (mbr.row_id = l_mbr.row_id)
   left join $Node4t_team_k7m_aux_d_crit_crmlkIN ext on ((ext.row_id = mbr.row_id) and (mbr.PARTY_TYPE_CD = ${SparkMain.partyTypeOrg}))
   left join $Node5t_team_k7m_aux_d_crit_crmlkIN pos1 on (pos1.row_id = ext.pr_postn_id)
   left join $Node6t_team_k7m_aux_d_crit_crmlkIN i on (i.PARTY_ID = mbr.ROW_ID)
   left join $Node6t_team_k7m_aux_d_crit_crmlkIN i2 on ((i2.LINK_ID = i.LINK_ID) and (i2.PARTY_ID <> i.PARTY_ID))
   left join $Node3t_team_k7m_aux_d_crit_crmlkIN mbr2 on (mbr2.row_id = i2.PARTY_ID)
   left join $Node4t_team_k7m_aux_d_crit_crmlkIN ext2 on ((ext2.row_id = mbr2.row_id) and (mbr2.PARTY_TYPE_CD = ${SparkMain.partyTypeOrg}))
   left join $Node7t_team_k7m_aux_d_crit_crmlkIN i3 on (i3.LINK_ID = i2.LINK_ID)
   left join $Node8t_team_k7m_aux_d_crit_crmlkIN ext_x1 on (mbr2.row_id = ext_x1.par_row_id)
   left join $Node8t_team_k7m_aux_d_crit_crmlkIN ext_x2 on (mbr.row_id = ext_x2.par_row_id)
   left join $Node9t_team_k7m_aux_d_crit_crmlkIN adm on (I3.CRIT_ID = adm.row_id)
       where gsz.status not in (${SparkMain.closedStatus})"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Prep").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_crmlkPrep")



    val createHiveTableStage2 = spark.sql(
      s"""    select distinct
          inn1,
          inn2,
          cast(1 as int) as quantity
     from (select inn1, inn2
             from $Nodet_team_k7m_aux_d_crit_crmlkPrep c
             left semi join $Node10t_team_k7m_aux_d_crit_crmlkIN bc
               on case when c.inn1 = bc.org_inn_crm_num then 1
                       when c.inn2 = bc.org_inn_crm_num then 1
                       else 0 end = 1)"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_crmlkOUT")

    logInserted()
    logEnd()
  }


}
