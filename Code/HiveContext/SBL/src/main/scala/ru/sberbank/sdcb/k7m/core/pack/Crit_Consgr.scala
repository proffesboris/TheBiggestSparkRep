package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql._

// 19.2 Расчет консолидированных групп с рейтингами
class Crit_Consgr(override val spark: SparkSession, val config: Config, date: LocalDate) extends EtlLogger with EtlJob {

  val stgSchema = config.stg
  val targetAuxSchema = config.aux
  val targetPaSchema = config.pa

  override val dashboardName = s"$targetAuxSchema.crit_consgr"
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}crit_consgr"


  def run(): Unit = {

    logStart()

    val date_cond = date

    val CX_CNSLD_GROUP_query = s"""
      select *
        from $stgSchema.crm_cx_cnsld_group
    """

    val CX_CNSLD_GROUP = spark.sql(CX_CNSLD_GROUP_query)
    CX_CNSLD_GROUP.createOrReplaceTempView("CX_CNSLD_GROUP")

    val CX_CG_ACCOUNT_query = s"""
    select *
    from $stgSchema.crm_cx_cg_account

    """
    val CX_CG_ACCOUNT = spark.sql(CX_CG_ACCOUNT_query)
    CX_CG_ACCOUNT.createOrReplaceTempView("CX_CG_ACCOUNT")

    val CONSGROUPS2_query = """
    select cons_gr.row_id as id_KG,
             cons_gr.name as KG_name,
             cg_account.account_id as crm_id
      from CX_CNSLD_GROUP cons_gr
      inner join CX_CG_ACCOUNT  cg_account
      on   cons_gr.row_id = cg_account.CG_ID
      where  cons_gr.status_cd = 'Действующая'
    """
    val CONSGROUPS2 = spark.sql(CONSGROUPS2_query)
    CONSGROUPS2.createOrReplaceTempView("CONSGROUPS2")

    val S_ORG_EXT_X_query = s"""
    select *
    from $stgSchema.crm_s_org_ext_x
    """
    val S_ORG_EXT_X = spark.sql(S_ORG_EXT_X_query)
    S_ORG_EXT_X.createOrReplaceTempView("S_ORG_EXT_X")

    val CONSGROUPS2INN_query = """
       select cons_gr.id_KG,
              org_x.sbrf_inn as inn
       from CONSGROUPS2 cons_gr
       left join S_ORG_EXT_X org_x
      on cons_gr.crm_id = org_x.par_row_id
      where org_x.sbrf_inn is not null
    """
    val CONSGROUPS2INN = spark.sql(CONSGROUPS2INN_query)
    CONSGROUPS2INN.createOrReplaceTempView("CONSGROUPS2INN")

    val CONSGROUPS_INN_LK_query = """
      select distinct left1.inn as left_inn,
                right1.inn as right_inn,
                left1.id_KG
      from CONSGROUPS2INN left1
      inner join CONSGROUPS2INN right1
      on (
              (left1.id_KG = right1.id_KG) and (left1.inn <> right1.inn)
       )
    """
    val CONSGROUPS_INN_LK = spark.sql(CONSGROUPS_INN_LK_query)
    CONSGROUPS_INN_LK.createOrReplaceTempView("CONSGROUPS_INN_LK")

    val crit_KG_query = s"""
     select  left_inn,
                right_inn,
                '%s' as dt,
                id_KG

                from CONSGROUPS_INN_LK
                where (left_inn in (select org_inn_crm_num from $targetAuxSchema.basis_client)) or
                        (right_inn in (select org_inn_crm_num from $targetAuxSchema.basis_client))

    """.format(date_cond)
    val crit_KG = spark.sql(crit_KG_query)
    crit_KG.createOrReplaceTempView("crit_KG")

    spark.table(s"$targetAuxSchema.armlirt_ratings_table").createOrReplaceTempView("armlirt_ratings")

    val sql_query = """
        select aa.left_inn, aa.right_inn,aa.org_gsz_id, aa.dt, bb.rating as quantity
        from
        (
            select a.left_inn, a.right_inn, b.org_gsz_id, a.dt, max(b.request_date) as max_req_date
            from crit_KG a
            inner join armlirt_ratings b
            on (a.dt = b.dt) and (a.id_KG = b.org_gsz_id)
            group by a.left_inn, a.right_inn, b.org_gsz_id, a.dt
        ) aa
        inner join armlirt_ratings bb
        on (aa.dt = bb.dt) and (aa.org_gsz_id = bb.org_gsz_id) and bb.request_date = aa.max_req_date
        """

    val crit_cons_gr = spark.sql(sql_query)

    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(dashboardName)

    logInserted()
    logEnd()
  }

}