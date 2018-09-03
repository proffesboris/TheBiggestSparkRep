package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SBL16Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Node1t_team_k7m_aux_d_crit_guarantorsIN = s"${config.stg}.CRM_S_OPTY"
  val Node2t_team_k7m_aux_d_crit_guarantorsIN = s"${config.stg}.CRM_S_SALES_METHOD"
  val Node3t_team_k7m_aux_d_crit_guarantorsIN = s"${config.stg}.CRM_S_OPTY_X"
  val Node4t_team_k7m_aux_d_crit_guarantorsIN = s"${config.stg}.CRM_S_STG"
  val Node5t_team_k7m_aux_d_crit_guarantorsIN = s"${config.stg}.CRM_S_REVN"
  val Node6t_team_k7m_aux_d_crit_guarantorsIN = s"${config.stg}.CRM_S_ORG_EXT"
  val Node7t_team_k7m_aux_d_crit_guarantorsIN = s"${config.stg}.CRM_S_ORG_EXT2_FNX"
  val Node8t_team_k7m_aux_d_crit_guarantorsIN = s"${config.stg}.CRM_S_ORG_EXT_X"
  val Node9t_team_k7m_aux_d_crit_guarantorsIN = s"${config.stg}.CRM_S_FN_OFFR_COLT" //s"${config.stg}.CRM_S_FN_OFFR_COLT"
  val Node10t_team_k7m_aux_d_crit_guarantorsIN = s"${config.aux}.basis_client"
  val Nodet_team_k7m_aux_d_crit_guarantorsOUT = s"${config.aux}.crit_guarantors"


  override val dashboardName: String = Nodet_team_k7m_aux_d_crit_guarantorsOUT //витрина
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_guarantors"

  def DoSBL16CritBorrowers(dateString: String) {

    Logger.getLogger(Nodet_team_k7m_aux_d_crit_guarantorsOUT).setLevel(Level.WARN)

    logStart()

    val Nodet_team_k7m_aux_d_crit_guarantorsBigTable = Nodet_team_k7m_aux_d_crit_guarantorsOUT.concat  ("_BigTable")
    val Nodet_team_k7m_aux_d_crit_guarantorsBigTable2 = Nodet_team_k7m_aux_d_crit_guarantorsOUT.concat ("_BigTable2")
    val Nodet_team_k7m_aux_d_crit_guarantorsCol = Nodet_team_k7m_aux_d_crit_guarantorsOUT.concat       ("Col")
    val Nodet_team_k7m_aux_d_crit_guarantorsBigTable3 = Nodet_team_k7m_aux_d_crit_guarantorsOUT.concat ("_BigTable3")
    val Nodet_team_k7m_aux_d_crit_guarantorsBorGuar = Nodet_team_k7m_aux_d_crit_guarantorsOUT.concat   ("_borr_guar")
    val Nodet_team_k7m_aux_d_crit_guarantorsBorGuarAct = Nodet_team_k7m_aux_d_crit_guarantorsOUT.concat("_borr_guar_act")

    val createHiveTableStage1 = spark.sql(
      s"""  select c.PR_DEPT_OU_ID as row_id,
                c.ROW_ID as opty_id,
                pr.ROW_ID as prod_id,
            --    pr.X_CREDIT_MODE,
                pr.X_LOAN_SRT,
                pr.X_LOAN_DUR_M_KI,
                pr.REVN_STAT_CD,
                f.name as stg,
                nvl(cast(e.ATTRIB_12 as string), '${dateString}') as dt
                --,nvl(e.ATTRIB_12, c.odsvalidfrom ) as dt_bigint
            from $Node1t_team_k7m_aux_d_crit_guarantorsIN c
            JOIN $Node2t_team_k7m_aux_d_crit_guarantorsIN d
              ON d.ROW_ID = c.SALES_METHOD_ID and d.name =${SparkMain.includeName}
            JOIN $Node3t_team_k7m_aux_d_crit_guarantorsIN e
              ON e.PAR_ROW_ID = c.ROW_ID
            JOIN $Node4t_team_k7m_aux_d_crit_guarantorsIN f
              ON f.ROW_ID = c.CURR_STG_ID
            JOIN $Node5t_team_k7m_aux_d_crit_guarantorsIN pr
              on pr.OPTY_ID = c.ROW_ID"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_BigTable").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_guarantorsBigTable")

    val createHiveTableStage2 = spark.sql(
      s"""  select
                o.ROW_ID
               ,o.opty_id
               ,o.prod_id
               ,o.dt
               ,a.name as org_name
               ,a.OU_TYPE_CD as CRM_TYPE
               ,o.stg
               ,o.REVN_STAT_CD
          --     ,o.X_CREDIT_MODE
               ,o.X_LOAN_SRT
               ,o.X_LOAN_DUR_M_KI
               ,b.sbrf_inn
               ,b.ATTRIB_07 as ridency
               ,c.attrib_03 as bsegment
         from $Node6t_team_k7m_aux_d_crit_guarantorsIN a
         join $Nodet_team_k7m_aux_d_crit_guarantorsBigTable o
           on a.row_id = o.row_id
          and a.OU_TYPE_CD not in (${SparkMain.excludeTypeCD})
         join $Node7t_team_k7m_aux_d_crit_guarantorsIN c
           on c.ROW_ID = o.ROW_ID
         join $Node8t_team_k7m_aux_d_crit_guarantorsIN b
           on b.ROW_ID = a.ROW_ID"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_BigTable2").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_guarantorsBigTable2")


    val createHiveTableStage3 = spark.sql(
      s"""select
              col.row_id
              ,col.X_LN_ACC_ID as COLL_X_LN_ACC_ID
              ,ci.sbrf_inn as COLL_X_LN_ACC_INN
              ,o.dt
              ,o.prod_id
              ,col.OPTYPRD_ID
              ,col.X_SORT_CD as COLL_X_SORT_CD
          from $Node9t_team_k7m_aux_d_crit_guarantorsIN col
          join $Nodet_team_k7m_aux_d_crit_guarantorsBigTable o
            on col.OPTYPRD_ID = o.prod_id
          left join $Node8t_team_k7m_aux_d_crit_guarantorsIN ci
            on col.X_LN_ACC_ID = ci.row_id
 """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_Col").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_guarantorsCol")

    val createHiveTableStage4 = spark.sql(
      s"""    select
                a.ROW_ID
               ,a.opty_id
               ,a.prod_id
               ,a.dt
               ,a.org_name
               ,a.CRM_TYPE
               ,a.stg
               ,a.REVN_STAT_CD
               ,a.X_LOAN_SRT
               ,a.X_LOAN_DUR_M_KI
               ,a.sbrf_inn
               ,a.ridency
               ,a.bsegment
               ,col.COLL_X_SORT_CD
               ,col.COLL_X_LN_ACC_ID
               ,col.COLL_X_LN_ACC_INN
         from $Nodet_team_k7m_aux_d_crit_guarantorsBigTable2 a
    left join $Nodet_team_k7m_aux_d_crit_guarantorsCol col
           on col.OPTYPRD_ID = a.prod_id and col.dt = a.dt
    """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_BigTable3").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_guarantorsBigTable3")

    val createHiveTableStage5 = spark.sql(
      s"""    select
             dt
             ,sbrf_inn as borrower_inn
             ,COLL_X_LN_ACC_INN as guarantor_inn
         from $Nodet_team_k7m_aux_d_crit_guarantorsBigTable3
        where stg in (${SparkMain.includeStg})
          and REVN_STAT_CD <> ${SparkMain.excludeRevnStatCD}
          and UPPER(COLL_X_SORT_CD) like ${SparkMain.includeCollXSortCD1}
          and UPPER(COLL_X_SORT_CD) like ${SparkMain.includeCollXSortCD2}
          and COLL_X_LN_ACC_ID is not null
    """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_borr_guar").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_guarantorsBorGuar")

    val createHiveTableStage6 = spark.sql(
      s"""select
       borrower_inn
      ,guarantor_inn
  from $Nodet_team_k7m_aux_d_crit_guarantorsBorGuar
 where dt < '${dateString}'
   and guarantor_inn <> '${SparkMain.innSber}'
   and borrower_inn <> '${SparkMain.innSber}'
    """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPath}_borr_guar_act").saveAsTable(s"$Nodet_team_k7m_aux_d_crit_guarantorsBorGuarAct")



    val createHiveTableStage7 = spark.sql(
      s""" select  distinct
      a.guarantor_inn as inn1,
      b.guarantor_inn as inn2,
      1 as quantity
  from $Nodet_team_k7m_aux_d_crit_guarantorsBorGuarAct a
  join $Nodet_team_k7m_aux_d_crit_guarantorsBorGuarAct b
    on a.borrower_inn = b.borrower_inn
  left semi join $Node10t_team_k7m_aux_d_crit_guarantorsIN f
    on case when a.guarantor_inn = f.org_inn_crm_num then 1
            when b.guarantor_inn = f.org_inn_crm_num then 1
            else 0 end = 1
  where (a.guarantor_inn <> b.guarantor_inn)"""
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_crit_guarantorsOUT")

    logInserted()
    logEnd()
  }


}
