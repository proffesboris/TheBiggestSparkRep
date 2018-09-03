package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

class PdPravoInClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  import spark.implicits._

  val Stg0Schema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_pa_d_pdpravoinIN = s"${Stg0Schema}.prv_CASE_PARTICIPANT"
  val Node2t_team_k7m_pa_d_pdpravoinIN = s"${Stg0Schema}.prv_CASES"
  val Node3t_team_k7m_pa_d_pdpravoinIN = s"${Stg0Schema}.prv_REF_SIDE_TYPE"
  val Node4t_team_k7m_pa_d_pdpravoinIN = s"${Stg0Schema}.prv_REF_CASE_TYPE"
  val Node5t_team_k7m_pa_d_pdpravoinIN = s"${Stg0Schema}.prv_REF_CASE_CATEGORY"
  val Node6t_team_k7m_pa_d_pdpravoinIN = s"${Stg0Schema}.prv_REF_BANKRUPTCY_STAGE"
  val Node7t_team_k7m_pa_d_pdpravoinIN = s"${Stg0Schema}.prv_REF_CASE_STAGE"
  val Nodet_team_k7m_pa_d_pdpravoinOUT = s"${MartSchema}.pdpravoin"
  val Nodet_team_k7m_pa_d_pdpravoin2OUT = s"${DevSchema}.pdpravoin"
  val dashboardPath = s"${config.paPath}pdpravoin"
  val dashboardPathStg = s"${config.auxPath}pdpravoin"


  override val dashboardName: String = Nodet_team_k7m_pa_d_pdpravoinOUT //витрина
  override def processName: String = "pdpravoin"

  def DoPdPravoIn (dateString: String)
  {
    Logger.getLogger(Nodet_team_k7m_pa_d_pdpravoinOUT).setLevel(Level.WARN)
    logStart()

    val Nodet_team_k7m_pa_d_pdpravoinStg1 = Nodet_team_k7m_pa_d_pdpravoin2OUT.concat("_Stg1")
    val Nodet_team_k7m_pa_d_pdpravoinStg2 = Nodet_team_k7m_pa_d_pdpravoin2OUT.concat("_Stg2")

    val createHiveTableStage1 = spark.sql(
      s"""select
          cp.ORG_INN				ORG_INN
         ,c.C_REG_DT				C_REG_DT
         ,cp.CASE_ID				CASE_ID
         ,CEIL(datediff(to_date('${dateString}'),to_date(c.C_REG_DT))/365) as YEAR_V
         ,bank.BANKRUPTCY_STAGE_ID BANKRUPTCY_STAGE_ID
         ,bank.BANKRUPTCY_STAGE_NM BANKRUPTCY_STAGE_NM
         ,cs.CASE_STAGE_ID CASE_STAGE_ID
         ,cs.CASE_STAGE_NM CASE_STAGE_NM
         ,side.SIDE_TYPE_ID SIDE_TYPE_ID
         ,side.SIDE_TYPE_NM SIDE_TYPE_NM
         ,c.C_SIMPLE_JUSTICE_FLG C_SIMPLE_JUSTICE_FLG
         ,c.C_CLAIM_SUM C_CLAIM_SUM
         ,c.C_CLAIM_TO_PAY C_CLAIM_TO_PAY
         ,c.CASE_TYPE_ID CASE_TYPE_ID
         ,c_type.CASE_TYPE_NM CASE_TYPE_NM
         ,c.CASE_CATEGORY_ID CASE_CATEGORY_ID
         ,c_cat.CASE_CATEGORY_NM	CASE_CATEGORY_NM
          from (
               select
                   effectivefrom
                  ,effectiveto
                  ,case_id
                  ,case_participant_id
                  ,cp_address
                  ,cp_birth_dt
                  ,cp_birth_place
                  ,cp_nm
                  ,cp_ogrn
                  ,cp_snils
                  ,file_dt
                  ,load_dt
                  ,org_inn
                  ,side_type_id
               from $Node1t_team_k7m_pa_d_pdpravoinIN
               where '${dateString}' >= effectiveFrom
               and '${dateString}' < effectiveTo
               ) as cp
         inner join (
               select
                     effectiveto
                    ,effectivefrom
                    ,bankruptcy_stage_id
                    ,c_claim_sum
                    ,c_claim_to_pay
                    ,c_court_nm
                    ,c_reg_dt
                    ,c_simple_justice_flg
                    ,case_category_id
                    ,case_id
                    ,case_num
                    ,case_stage_id
                    ,case_type_id
                    ,file_dt
                    ,load_dt
               from $Node2t_team_k7m_pa_d_pdpravoinIN
               where '${dateString}' >= effectiveFrom
               and '${dateString}' < effectiveTo
                ) as c
           on cp.CASE_ID = c.CASE_ID
         inner join (
               select
                 file_dt
               , load_dt
               , side_type_id
               , side_type_nm
               , effectivefrom
               , ROW_NUMBER() OVER(PARTITION BY SIDE_TYPE_ID ORDER BY effectiveFrom DESC) as rn
               from $Node3t_team_k7m_pa_d_pdpravoinIN
               where effectiveFrom <= '${dateString}'
                ) as side
           on cp.SIDE_TYPE_ID = side.SIDE_TYPE_ID and side.rn = 1
         inner join (
               select *, ROW_NUMBER() OVER(PARTITION BY CASE_TYPE_ID ORDER BY effectiveFrom DESC) as rn
               from $Node4t_team_k7m_pa_d_pdpravoinIN
               where effectiveFrom <= '${dateString}'
                ) as c_type
           on c.CASE_TYPE_ID = c_type.CASE_TYPE_ID and c_type.rn = 1
         inner join (
               select *, ROW_NUMBER() OVER(PARTITION BY CASE_CATEGORY_ID ORDER BY effectiveFrom DESC) as rn
               from $Node5t_team_k7m_pa_d_pdpravoinIN
               where effectiveFrom <= '${dateString}'
                ) as c_cat
           on c.CASE_CATEGORY_ID = c_cat.CASE_CATEGORY_ID and c_cat.rn = 1
         inner join (
               select *,ROW_NUMBER() OVER(PARTITION BY BANKRUPTCY_STAGE_ID ORDER BY effectiveFrom DESC) as rn
               from $Node6t_team_k7m_pa_d_pdpravoinIN
               where effectiveFrom <= '${dateString}'
                ) as bank
           ON c.BANKRUPTCY_STAGE_ID = bank.BANKRUPTCY_STAGE_ID and bank.rn = 1
         inner join (
               select *,ROW_NUMBER() OVER(PARTITION BY CASE_STAGE_ID ORDER BY effectiveFrom DESC) as rn
               from $Node7t_team_k7m_pa_d_pdpravoinIN
               where effectiveFrom <= '${dateString}'
         	  ) as cs
           ON c.CASE_STAGE_ID = cs.CASE_STAGE_ID and cs.rn = 1

         WHERE CEIL(datediff(TO_DATE('${dateString}'),to_date(c.C_REG_DT))/365)<=5
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPathStg}_Stg1")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_pdpravoinStg1")

    val createHiveTableStage2 = spark.sql(
      s"""
         SELECT DISTINCT
           CASE_ID
         FROM
           (select
                 ORG_INN
                 ,CASE_ID
                 ,count(1) as ROW_CNT
             from
                 $Nodet_team_k7m_pa_d_pdpravoinStg1
             group by
                  ORG_INN
                 ,CASE_ID
           ) AS BC
         WHERE BC.ROW_CNT!=1
    """
    )
    createHiveTableStage2
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${dashboardPathStg}_Stg2")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_pdpravoinStg2")


    val createHiveTableStage3 = spark.table(Nodet_team_k7m_pa_d_pdpravoinStg1)
            .join(spark.table(Nodet_team_k7m_pa_d_pdpravoinStg2),Seq("CASE_ID"),"left_anti")
            .select(
               $"CASE_ID"
              ,$"ORG_INN"
              ,$"C_REG_DT"
              ,$"C_SIMPLE_JUSTICE_FLG"
              ,$"CASE_CATEGORY_ID"
              ,$"CASE_TYPE_ID"
              ,$"BANKRUPTCY_STAGE_ID"
              ,$"BANKRUPTCY_STAGE_NM"
              ,$"CASE_CATEGORY_NM"
              ,$"CASE_STAGE_ID"
              ,$"CASE_STAGE_NM"
              ,$"CASE_TYPE_NM"
              ,$"SIDE_TYPE_ID"
              ,$"SIDE_TYPE_NM"
              ,$"C_CLAIM_SUM".cast(DoubleType)
              ,$"C_CLAIM_TO_PAY".cast(DoubleType)
              ,$"YEAR_V".cast(IntegerType)
              ,lit(dateString).as("CALC_DATE")
                 )
    createHiveTableStage3
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"$dashboardPath")
      .saveAsTable(s"$Nodet_team_k7m_pa_d_pdpravoinOUT")

    logInserted()
    logEnd()
  }
}

