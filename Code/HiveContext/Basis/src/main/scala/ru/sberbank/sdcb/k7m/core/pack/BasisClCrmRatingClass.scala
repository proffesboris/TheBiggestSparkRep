package ru.sberbank.sdcb.k7m.core.pack

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class BasisClCrmRatingClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {
  val Stg0Schema = config.stg
  val DevSchema = config.aux

  //------------------CRM-----------------------------
  val Node1t_team_k7m_aux_d_basis_client_crm_ratingIN = s"${DevSchema}.basis_client_crm"
  val Node2t_team_k7m_aux_d_basis_client_crm_ratingIN = s"${Stg0Schema}.crm_CX_RATING"
  val Node3t_team_k7m_aux_d_basis_client_crm_ratingIN = s"${Stg0Schema}.crm_CX_RAT_RAT"
  val Node4t_team_k7m_aux_d_basis_client_crm_ratingIN = s"${Stg0Schema}.crm_CX_RAT_RAT_VAL"
  val Node5t_team_k7m_aux_d_basis_client_crm_ratingIN = s"${Stg0Schema}.crm_CX_RATING_MODEL"
  val Nodet_team_k7m_aux_d_basis_client_crm_ratingOUT = s"${DevSchema}.basis_client_crm_rating"
  val dashboardPath = s"${config.auxPath}basis_client_crm_rating"


  override val dashboardName: String = Nodet_team_k7m_aux_d_basis_client_crm_ratingOUT //витрина
  override def processName: String = "Basis"

  def DoBasisClCrmRating()//(spark:org.apache.spark.sql.SparkSession)
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_basis_client_crm_ratingOUT).setLevel(Level.WARN)

    logStart()

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
         select a1.object_id,
                a1.row_id,
                a1.approved_date as RATING_APPR_DT,
                a1.final_rating  as RATING_MAIN_PROCESS,
                a1.default_prob  as PD_MAIN_PROCESS,
                A5.NAME          as RATING_MODEL_NAME,
                a3.value         as RATING_MAIN_PROCESS_PRICE,
                a4.value         as RATING_MAIN_PROCESS_REZERV
           from $Node1t_team_k7m_aux_d_basis_client_crm_ratingIN cl
           join (select object_id,
                        row_id,
                        approved_date,
                        final_rating,
                        default_prob,
                        model_id
                   from (select object_id,
                                row_id,
                                approved_date,
                                final_rating,
                                default_prob,
                                model_id,
                                row_number() over(partition by object_id order by approved_date desc) rn
                           from $Node2t_team_k7m_aux_d_basis_client_crm_ratingIN
                          where status = 'Актуальный'
                            and project is null --замена = ''
                            and approved_date is not null) o
                  where o.rn = 1) a1 on (cl.org_id = a1.object_id)
         		   LEFT JOIN (select RATING_ID,
                                      name,
                                      value
                                 from (select a2.RATING_ID,
                                      a3.name,
                                      a3.value,
                                      row_number() over(partition by a2.RATING_ID order by a3.LAST_UPD desc) rn
                                 from $Node3t_team_k7m_aux_d_basis_client_crm_ratingIN a2
                                 JOIN $Node4t_team_k7m_aux_d_basis_client_crm_ratingIN a3 on (a3.PARENT_ID = a2.ROW_ID and a3.name = 'RATING_PRICE')) o1
                                where o1.rn = 1) a3 on (a3.RATING_ID = a1.ROW_ID)
                    LEFT JOIN (select RATING_ID,
                                     name,
                                      value
                                 from (select a2.RATING_ID,
                                      a3.name,
                                      a3.value,
                                      row_number() over(partition by a2.RATING_ID order by a3.LAST_UPD desc) rn
                                 from $Node3t_team_k7m_aux_d_basis_client_crm_ratingIN a2
                                 JOIN $Node4t_team_k7m_aux_d_basis_client_crm_ratingIN a3
                                   on (a3.PARENT_ID = a2.ROW_ID and a3.name = 'RATING_REZERV')) o2
                                 where o2.rn = 1) a4 on (a4.RATING_ID = a1.ROW_ID)
                    LEFT JOIN $Node5t_team_k7m_aux_d_basis_client_crm_ratingIN a5  on (a5.row_id = a1.model_id)
       """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode("overwrite")
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_basis_client_crm_ratingOUT")

    logInserted()
    logEnd()

  }
}
