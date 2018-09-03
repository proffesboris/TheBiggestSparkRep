package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql._

///19. Консолидированные группы
///19.1 Получение рейтингов КГ из АРМЛиРТ
class Armlirt_Ratings(override val spark: SparkSession, val config: Config, date: LocalDate) extends EtlLogger with EtlJob {

  val stgSchema = config.stg
  val targetAuxSchema = config.aux
  val targetPaSchema = config.pa

  override val dashboardName = s"$targetAuxSchema.armlirt_ratings_table"
  override def processName: String = "SBL"
  val dashboardPath = s"${config.auxPath}armlirt_ratings_table"


  def run(): Unit = {

    logStart()

    val date_cond = date

    val armlirt_org_query = s"""
                      SELECT organization_id,
                          max(inn) as org_inn,
                          max(kpp) as org_kpp,
                          max(upper(name)) as org_name,
                          max(code) as org_code
                      from $stgSchema.amr_organizations
                      group by organization_id
                    """

    val temp_armlirt_org = spark.sql(armlirt_org_query)
    temp_armlirt_org.createOrReplaceTempView("temp_armlirt_org")

    val armlirt_coborrower_groups_query = s"""
                      SELECT coborrower_group_id,
                             max(upper(name)) AS gsz_name,
                              max(code) AS gsz_code
                      FROM $stgSchema.amr_coborrower_groups
                      group by coborrower_group_id
                      """

    val temp_armlirt_coborrower_groups = spark.sql(armlirt_coborrower_groups_query)
    temp_armlirt_coborrower_groups.createOrReplaceTempView("temp_armlirt_coborrower_groups")

    val armlirt_requests_query = s"""
                                    select *, from_unixtime(cast(request_dt as int )) as request_date_h
                                    FROM $stgSchema.amr_requests
                                  """
    val temp_armlirt_requests = spark.sql(armlirt_requests_query)
    temp_armlirt_requests.createOrReplaceTempView("temp_armlirt_requests")

    val armlirt_sl_query = """
                          SELECT
                                CASE WHEN calc_req.organization_id is NULL THEN NULL ELSE calc_req.organization_id END AS org_id
                               , CASE WHEN calc_req.organization_id is NULL THEN calc_req.coborrower_group_id ELSE NULL END AS gsz_id
                               , CASE WHEN calc_req.organization_id is NULL THEN 'GSZ_ID' ELSE 'ORG_ID' END AS gsz_org
                               , calc_req.coborrower_group_id
                               ,calc_req.organization_id
                                , fin_req.request_id r_id
                                , CALC_REQ.PROJECT_ID
                                , calc_req.request_id

                           FROM         temp_armlirt_requests   fin_req
                             INNER JOIN   temp_armlirt_requests calc_req
                               WHERE fin_req.rating_identifier = calc_req.rating_identifier
                                        AND calc_req.request_type_id IN (2, 3)
                                        AND calc_req.is_process_failed = 'N'
                                        AND fin_req.request_type_id = 5
                                        AND fin_req.is_process_failed = 'N'
                                        AND fin_req.request_date_h < '%s'
                                        and calc_req.request_date_h <= fin_req.request_date_h

                        """.format(date_cond)

    val temp_armlirt_sl = spark.sql(armlirt_sl_query)
    temp_armlirt_sl.createOrReplaceTempView("temp_armlirt_sl")

    val armlirt_sl_final_query = """
        SELECT
            org_id
            ,gsz_id
            ,gsz_org
            ,r_id
            ,MAX(PROJECT_ID) as pr_id
            ,MAX(request_id) as cr_id
        from temp_armlirt_sl
        group by org_id, gsz_id, gsz_org, r_id

    """

    val temp_armlirt_sl_final = spark.sql(armlirt_sl_final_query)
    temp_armlirt_sl_final.createOrReplaceTempView("temp_armlirt_sl_final")

    val armlirt_org_model_dt_query = """
                SELECT
                        sl.org_id
                        ,sl.gsz_org
                        ,sl.gsz_id
                        ,orgs.org_inn
                        ,orgs.org_kpp
                        ,orgs.org_name
                        ,orgs.org_code
                        ,gsz_name
                        ,gsz_code
                        ,sl.r_id
                        ,sl.cr_id
                        ,sl.pr_id
                FROM temp_armlirt_sl_final sl
                left join temp_armlirt_org orgs
                on orgs.organization_id = sl.org_id
                left join temp_armlirt_coborrower_groups gsz
                on gsz.coborrower_group_id = sl.gsz_id
                WHERE (sl.org_id IS NOT NULL) OR (sl.gsz_id IS NOT NULL)
              """
    val temp_armlirt_org_model_dt = spark.sql(armlirt_org_model_dt_query)
    temp_armlirt_org_model_dt.createOrReplaceTempView("temp_armlirt_org_model_dt")

    val armlirt_ratings_query = s"""
    select *
      FROM $stgSchema.amr_ratings
    """

    val temp_armlirt_ratings = spark.sql(armlirt_ratings_query)
    temp_armlirt_ratings.createOrReplaceTempView("temp_armlirt_ratings")

    val armlirt_stage_data_query = s"""
    select *
    FROM $stgSchema.amr_stage_data
    """

    val temp_armlirt_stage_data = spark.sql(armlirt_stage_data_query)
    temp_armlirt_stage_data.createOrReplaceTempView("temp_armlirt_stage_data")


    val armlirt_request_data_query = s"""
    select *
    FROM $stgSchema.amr_request_data
    """

    val temp_armlirt_request_data = spark.sql(armlirt_request_data_query)
    temp_armlirt_request_data.createOrReplaceTempView("temp_armlirt_request_data")

    val armlirt_pd_rating_final_query = s"""
    select *
    FROM $stgSchema.amr_pd_rating_final
    """

    val temp_armlirt_pd_rating_final = spark.sql(armlirt_pd_rating_final_query)
    temp_armlirt_pd_rating_final.createOrReplaceTempView("temp_armlirt_pd_rating_final")

    val sql_query = """
                SELECT
 |                org_model_dt.r_id AS fin_request_id
 |                ,org_model_dt.cr_id AS calc_request_id
 |                ,from_unixtime(cast(r.request_dt as int)) as request_date
 |                ,CASE  WHEN org_model_dt.gsz_org = 'ORG_ID' THEN org_model_dt.ORG_code WHEN org_model_dt.gsz_org = 'GSZ_ID' THEN org_model_dt.gsz_code END AS org_gsz_id
 |                ,CASE  WHEN org_model_dt.gsz_org = 'ORG_ID' THEN org_model_dt.org_name WHEN org_model_dt.gsz_org = 'GSZ_ID' THEN org_model_dt.gsz_name END AS organization_name
 |                ,R.RATING_IDENTIFIER as rating_id
 |                ,CASE WHEN org_model_dt.gsz_org  = 'ORG_ID' THEN org_model_dt.org_INN ELSE NULL END AS inn
 |                ,CASE WHEN org_model_dt.gsz_org = 'ORG_ID' THEN org_model_dt.org_kpp ELSE NULL END AS kpp
 |                ,r.model_integration_uid AS model_code
 |                ,rate.code AS rating
 |                ,'%s' as dt
 |                ,max(from_unixtime(unix_timestamp(rd.report_date, 'dd.MM.yyyy')))  as rep_date
 |                ,max(from_unixtime(unix_timestamp(rd.start_date_str, 'dd.MM.yyyy')))  as st_date
 |                ,max(from_unixtime(unix_timestamp(rd.end_date_str, 'dd.MM.yyyy')))  as end_date
 |        FROM  temp_armlirt_pd_rating_final pdf
 |
 |          INNER JOIN  temp_armlirt_requests r
 |                    ON pdf.request_id = r.request_id
 |
 |          INNER JOIN   temp_armlirt_ratings  rate
 |                    ON pdf.rating_id = rate.rating_id
 |
 |          INNER JOIN   temp_armlirt_org_model_dt  org_model_dt
 |                    on (r.request_id = org_model_dt.r_id)
 |
 |          INNER JOIN   temp_armlirt_stage_data  sd
 |                    ON org_model_dt.cr_id = sd.request_id
 |
 |          LEFT JOIN    temp_armlirt_request_data  rd
 |                    ON (sd.model_parameter_id = rd.model_parameter_id) AND (sd.request_id = rd.request_id)
 |            WHERE
 |                SD.STAGE_ID = 3
 |                 and r.request_dt < cast (date '%s' as timestamp)
 |
 |        group by org_model_dt.r_id,
 |                   org_model_dt.cr_id ,
 |                 r.request_dt,
 |                 CASE  WHEN org_model_dt.gsz_org = 'ORG_ID' THEN org_model_dt.ORG_code WHEN org_model_dt.gsz_org = 'GSZ_ID' THEN org_model_dt.gsz_code END  ,
 |                 CASE  WHEN org_model_dt.gsz_org = 'ORG_ID' THEN org_model_dt.org_name WHEN org_model_dt.gsz_org = 'GSZ_ID' THEN org_model_dt.gsz_name END ,
 |                 R.RATING_IDENTIFIER ,
 |                 CASE WHEN org_model_dt.gsz_org  = 'ORG_ID' THEN org_model_dt.org_INN ELSE NULL END,
 |                 CASE WHEN org_model_dt.gsz_org = 'ORG_ID' THEN org_model_dt.org_kpp ELSE NULL END,
 |                 r.model_integration_uid,
 |                 rate.code
    """.format(date_cond, date_cond).stripMargin

    spark.sql(sql_query)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(dashboardName)

    logInserted()
    logEnd()
  }

}