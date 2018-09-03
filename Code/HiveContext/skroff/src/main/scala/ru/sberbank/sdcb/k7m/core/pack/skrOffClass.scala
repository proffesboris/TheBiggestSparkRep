package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{SaveMode, SparkSession}

class skrOffClass(val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

    val Stg0Schema = config.stg
    val DevSchema = config.aux
    val MartSchema = config.pa

    val tblBoGen = s"${config.aux}.bo_gen"
    val tblBoSKR = s"${config.aux}.bo_skr"

  val Node1t_team_k7m_pa_d_boIN = s"${MartSchema}.bo_keys"
    val Node2t_team_k7m_pa_d_boIN = s"${MartSchema}.clu"
    val Node3t_team_k7m_pa_d_boIN = s"${DevSchema}.amr_country_ckp"
    val Node5t_team_k7m_pa_d_boIN = s"${MartSchema}.score"
    val Node6t_team_k7m_pa_d_boIN = s"${DevSchema}.amr_time_discount"
    val dashboardPath = s"${config.auxPath}bo_skr"


    override val dashboardName: String = tblBoSKR //витрина
    override def processName: String = "SKROFF"

    def DoSKROff() {
    import SkrOffMain._

      logStart()

//      val createHiveTableStage0 = spark.sql(
//        s"""select
//      cast (null as string) as exec_id,
//      cast (null as bigint) as okr_id,
//      cast (null as string) as bo_id,
//      cast (null as string) as u7m_id,
//      cast (null as string) as clust_id,
//      cast (null as string) as key,
//      cast (null as string) as value_c,
//      cast(null as decimal(16,5)) value_n,
//      cast (null as timestamp) as value_d
//  from $Node1t_team_k7m_pa_d_boIN
// where 1=0
//  """
//      ).write
//        .format("parquet")
//        .mode(SaveMode.Append)
//        .option("path", dashboardPath)
//        .saveAsTable(s"$Nodet_team_k7m_pa_d_boOUT")

      spark.table(s"${config.aux}.bo_empty")
        .write
        .mode(SaveMode.Overwrite)
        .option("path", s"${config.paPath}bo_skr")
        .format("parquet")
        .saveAsTable(tblBoSKR)

      spark.sql(s"""INSERT INTO $tblBoSKR
                  |SELECT
                  |        T1.EXEC_ID
                  |        ,T1.OKR_ID
                  |		,T1.BO_ID
                  |		,T1.U7M_ID
                  |		,T1.CLUST_ID
                  |		,'CCF_C_OFFLINE' as KEY
                  |	 ,NULL as VALUE_C
                  |  ,CAST(CASE
                  |			WHEN T2.TYPE_CD = 'D' then T2.CCFp_MAX
                  |			ELSE 1
                  |		END as decimal(16,4)) as VALUE_N
                  |		,NULL as VALUE_D
                  |  FROM
                  |		${config.pa}.bo_keys T1
                  |		INNER JOIN (
                  |		    SELECT
                  |            		T_BO.BO_ID as BO_ID
                  |            		,T_CRDMODE.TYPE_CD as TYPE_CD
                  |            		,MAX(
                  |            			CAST(
                  |            				CASE
                  |            					WHEN T2.ccfp_val like '%byDuration' then CASE
                  |            													WHEN T_BO.DUR_U <= 12 then substr(T2.ccfp_val, 1, 3)
                  |            													WHEN T_BO.DUR_U > 12 then substr(T2.ccfp_val, 5, 3)
                  |            													END
                  |            					ELSE ccfp_val END
                  |            				as DECIMAL(16, 4)
                  |            			)
                  |            		) as CCFp_MAX
                  |              FROM
                  |                    ${config.pa}.bo_keys T1
                  |            		INNER JOIN (
                  |            			SELECT
                  |            					BO_ID
                  |            					,max(case WHEN key='CREDIT_MODE' then value_c else null end) as CREDIT_MODE
                  |            					,max(case WHEN key='PRODUCT' then value_c else null end) as PRODUCT
                  |            					,max(case WHEN key='CREDIT_INSTRUMENT' then value_c else null end) as CREDIT_INSTRUMENT
                  |            					,max(case WHEN key='DUR_U' then value_n else null end) as DUR_U
                  |            			  FROM
                  |            					$tblBoGen
                  |            			 GROUP BY
                  |            					BO_ID
                  |            		) T_BO ON (T1.BO_ID = T_BO.BO_ID)
                  |            		INNER JOIN
                  |            		(
                  |            			SELECT
                  |            					CASE crd_mode_cd
                  |            						WHEN 'Кредитный договор' then 'Credit Contract'
                  |            						WHEN 'НКЛ' then 'NKL'
                  |            						WHEN 'ВКЛ' then 'VKL'
                  |            						WHEN 'Договор об овердрафт. кредите' then 'Contract Credit Overdraft'
                  |            					END as CREDIT_MODE
                  |            					,CASE loan_sort_num
                  |            						WHEN 'Кредит' then 'Credit'
                  |            					END as CREDIT_INSTRUMENT
                  |            					,type_cd
                  |            			  FROM
                  |            					${config.aux}.amr_crd_mode
                  |            		) T_CRDMODE ON T_BO.CREDIT_MODE = T_CRDMODE.CREDIT_MODE and T_BO.CREDIT_INSTRUMENT = T_CRDMODE.CREDIT_INSTRUMENT
                  |            		INNER JOIN (
                  |            			SELECT
                  |            					case prd_name
                  |            						when 'Корпоративное кредитование' then '1-M813DK'
                  |            						when 'Индивидуальный овердрафт' then '1-M813EC'
                  |            					END AS PRODUCT
                  |            					,ccfp_val
                  |            			  FROM
                  |            					${config.aux}.amr_ead_prd
                  |            			 WHERE
                  |            					prd_stts = 'active'
                  |            		) T2 ON T_BO.PRODUCT = T2.PRODUCT
                  |             group BY
                  |                    T_BO.BO_ID
                  |            		,T_CRDMODE.TYPE_CD
                  |		) T2 ON T1.BO_ID=T2.BO_ID""".stripMargin)
      
      spark.sql(s"""insert into $tblBoSKR
                  |select
                  |        a.exec_id,
                  |        a.okr_id,
                  |        a.bo_id,
                  |        a.u7m_id,
                  |        a.clust_id,
                  |        'LGD_OFFLINE_PRICE' as key,
                  |        null as value_c,
                  |        cast(b.lgdpricing as decimal(16,5)) as value_n,
                  |        null as value_d
                  |  from 
                  |        ${config.pa}.bo_keys a
                  |        join ${config.pa}.lgdout b on (a.bo_id = b.bo_id)
                  |""".stripMargin)

      val createHiveTableStage1 = spark.sql(
        s"""select
        a.exec_id,
        a.okr_id,
        a.bo_id,
        a.u7m_id,
        a.clust_id,
        $keyElte as key,
        cast (null as string) as value_c,
        case
            when c.country_cd = 'RUS' or bo.value_c = c.currency_cd then 0
            else cast(coalesce(sc.pte, 0) * coalesce(sc.lgte, 0) as decimal(16,5))
        end as value_n,
        cast (null as timestamp) as value_d
  from
        $Node1t_team_k7m_pa_d_boIN a
        join $Node2t_team_k7m_pa_d_boIN b on a.u7m_id = b.u7m_id
        join $Node3t_team_k7m_pa_d_boIN c on b.reg_country = c.country_cd
        join $tblBoGen bo on bo.bo_id = a.bo_id and bo.key = $keyCur
        left join (
            select
                    u7m_id,
                    max(case when key = $keyPte then value_n else null end) as pte,
                    max(case when key = $keyLgte then value_n else null end) as lgte
              from
                    $Node5t_team_k7m_pa_d_boIN
             group by
                    u7m_id
        ) sc on sc.u7m_id = b.u7m_id"""
      ).persist()

      logInserted(count=createHiveTableStage1.count())

      createHiveTableStage1.write
              .format("parquet")
              .option("path", dashboardPath)
              .insertInto(tblBoSKR)

      createHiveTableStage1.unpersist()

      val createHiveTableStage2 = spark.sql(
        s"""
        select
      a.exec_id,
      a.okr_id,
      a.bo_id,
      a.u7m_id,
      a.clust_id,
      $keyKDiscount as key,
      cast (null as string)  as value_c,
      case
          when abs(c.max_term - c.min_term) > 0 then round(c.kmin + ((c.kmax - c.kmin)*(coalesce(bo.value_n, c.min_term) - c.min_term))/(c.max_term - c.min_term), 2)
          else 1
      end as value_n,
      cast (null as timestamp) as value_d
  from $Node1t_team_k7m_pa_d_boIN a
  join $tblBoGen bo
    on bo.bo_id = a.bo_id
   and bo.key = $keyDurU
  join $Node6t_team_k7m_pa_d_boIN c
    on 1=1
 where bo.value_n >= c.min_term and
       bo.value_n < c.max_term"""
      ).persist()

      logInserted(count=createHiveTableStage2.count())

      createHiveTableStage2.write
        .format("parquet")
        .option("path", dashboardPath)
        .insertInto(tblBoSKR)

      createHiveTableStage2.unpersist()

      val createHiveTableStage3 = spark.sql(
        s"""
        select
        a.exec_id,
        a.okr_id,
        a.bo_id,
        a.u7m_id,
        a.clust_id,
        $keyElpCoef as key,
        cast (null as string) as value_c,
        bo.value_n,
        cast (null as timestamp) as value_d
  from $Node1t_team_k7m_pa_d_boIN a
  join $tblBoSKR bo on bo.bo_id = a.bo_id
 where
        bo.key = $keyCcfCOffline """
      ).persist()

      logInserted(count=createHiveTableStage3.count())

      createHiveTableStage3.write
        .format("parquet")
        .option("path", dashboardPath)
        .insertInto(tblBoSKR)

      createHiveTableStage3.unpersist()

      val createHiveTableStage4 = spark.sql(
        s"""
        select
        a.exec_id,
        a.okr_id,
        a.bo_id,
        a.u7m_id,
        a.clust_id,
        $keySkrOffline as key,
        cast (null as timestamp) as value_c,
        greatest(0.01, round((b.value_n * bo.lgd + bo.elte) * bo.elp_coef * bo.k_discount, 2)) as value_n,
        cast (null as timestamp)  as value_d
  from $Node1t_team_k7m_pa_d_boIN a
  left join (
            select
                    bo_id,
                    max(case when key = $keyElte then value_n else null end) as elte,
                    max(case when key = $keyLGDOfflinePrice then value_n else null end) as lgd,
                    max(case when key = $keyElpCoef then value_n else null end) as elp_coef,
                    max(case when key = $keyKDiscount then value_n else null end) as k_discount
              from (
                  select bo_id, key,value_n  from $tblBoSKR  where key in ( $keyElte, $keyElpCoef, $keyKDiscount, $keyLGDOfflinePrice )
                    ) x
             group by
                    bo_id
        ) bo
    on bo.bo_id = a.bo_id
  join $Node5t_team_k7m_pa_d_boIN b
    on b.u7m_id = a.u7m_id
   and b.key = $keyPDOfflinePrice"""
      ).persist()

      logInserted(count=createHiveTableStage4.count())

      createHiveTableStage4.write
        .format("parquet")
        .option("path", dashboardPath)
        .insertInto(tblBoSKR)


      createHiveTableStage4.unpersist()

      logEnd()
    }
}

