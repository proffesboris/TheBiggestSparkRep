package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ScoreClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val StgSchema = config.stg
  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_scoreIN = s"${DevSchema}.pdout_kv"
  val Node2t_team_k7m_aux_d_scoreIN = s"${MartSchema}.score_keys"
  val Node3t_team_k7m_aux_d_scoreIN = s"${MartSchema}.limitout"
  val Node4t_team_k7m_aux_d_scoreIN = s"${DevSchema}.proxyrs_int_final"
  val Node5t_team_k7m_aux_d_scoreIN = s"${MartSchema}.CLU"
  val Node6t_team_k7m_aux_d_scoreIN = s"${DevSchema}.amr_country_ckp"
  val Node7t_team_k7m_aux_d_scoreIN = s"${DevSchema}.ProxyLGDin_STG_14_00_CALC_CUST"
  val Node8t_team_k7m_aux_d_scoreIN = s"${DevSchema}.etl_exec_stts"
  val Node9t_team_k7m_aux_d_scoreIN = s"${MartSchema}.proxylgdin"
  val Node10t_team_k7m_aux_d_scoreIN = s"${StgSchema}.ods_t_check_010"
  val Node11t_team_k7m_aux_d_scoreIN = s"${StgSchema}.eks_z_calc_over_lim"
  val Node12t_team_k7m_aux_d_scoreIN = s"${StgSchema}.ods_t_check_039"
  val Nodet_team_k7m_aux_d_scoreOUT = s"${MartSchema}.score"
  val dashboardPath = s"${config.paPath}score"

  private val bDateDt = LoggerUtils.obtainRunDtTimestamp(spark, config.aux)

  override val dashboardName: String = Nodet_team_k7m_aux_d_scoreOUT //витрина
  override def processName: String = "score"

  def DoScore()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_scoreOUT).setLevel(Level.WARN)

    logStart()

    //шаг1 - "переворачиваем" результат работы модели ГВ
    ToKeyValue.gen(config.pa,"pdout", config.aux,"pdout_kv")(spark, config.aux, config.auxPath)

    //шаг2 - создание таблицы score
    val createHiveTableStage1 = spark.sql(
      s"""
         select
         		cast(a.exec_id as string) as exec_id,
         		cast(a.okr_id as string) as okr_id,
         		cast(a.obj_id as string) as u7m_id,
         		a.key,
         		a.value_c,
         		cast(a.value_n as decimal(16, 4)) as value_n,
         		a.value_d
           from
         		$Node1t_team_k7m_aux_d_scoreIN a
         		join $Node2t_team_k7m_aux_d_scoreIN b on (a.obj_id = b.u7m_id)
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг3 - добавление параметров SKE. Дописать результат в созданную таблицу score
    val createHiveTableStage2 = spark.sql(
    s"""select
         	cast(a.exec_id as string) as exec_id,
          cast(null as string) as okr_id,
          cast(a.u7m_id as string) as u7m_id,
         	'SKE_BASE' as key,
          cast(null as string) as value_c,
         	cast(coalesce(a.ske_base, 0) as decimal(16,4)) as value_n,
          cast(null as timestamp) as value_d
       from
         	$Node3t_team_k7m_aux_d_scoreIN a
         	join $Node2t_team_k7m_aux_d_scoreIN b on (a.u7m_id = b.u7m_id)
       where
       		a.ske_base is not NULL
    """
  )
    createHiveTableStage2
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage2_1 = spark.sql(
      s"""select
         	cast(a.exec_id as string) as exec_id,
          cast(null as string) as okr_id,
          cast(a.u7m_id as string) as u7m_id,
         	'SKE_D_0' as key,
          cast(null as string) as value_c,
         	cast(coalesce(a.ske_d_0, 0) as decimal(16,4)) as value_n,
          cast(null as timestamp) as value_d
      from
         	$Node3t_team_k7m_aux_d_scoreIN a
         	join $Node2t_team_k7m_aux_d_scoreIN b on (a.u7m_id = b.u7m_id)
       where
        	a.ske_d_0 is not NULL
    """
    )
    createHiveTableStage2_1
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage2_2 = spark.sql(
      s"""select
         	cast(a.exec_id as string) as exec_id,
          cast(null as string) as okr_id,
          cast(a.u7m_id as string) as u7m_id,
         	'ACTREPORT_FLAG' as key,
          cast(null as string) as value_c,
         	cast(a.ACTREPORT_FLAG as decimal(16,4)) as value_n,
          cast(null as timestamp) as value_d
      from
         	$Node3t_team_k7m_aux_d_scoreIN a
         	join $Node2t_team_k7m_aux_d_scoreIN b on (a.u7m_id = b.u7m_id)
       where
        	a.actreport_flag is not NULL
    """
    )
    createHiveTableStage2_2
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage2_3 = spark.sql(
      s"""select
         	cast(a.exec_id as string) as exec_id,
          cast(null as string) as okr_id,
          cast(a.u7m_id as string) as u7m_id,
         	'SKE_DebtCredit0' as key,
          cast(null as string) as value_c,
         	cast(coalesce(a.SKE_DebtCredit0, 0) as decimal(16,4)) as value_n,
          cast(null as timestamp) as value_d
       from
         	$Node3t_team_k7m_aux_d_scoreIN a
         	join $Node2t_team_k7m_aux_d_scoreIN b on (a.u7m_id = b.u7m_id)
       where
        	a.ske_debtcredit0 is not NULL
    """
    )
    createHiveTableStage2_3
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage2_4 = spark.sql(
      s"""select
         	cast(a.exec_id as string) as exec_id,
          cast(null as string) as okr_id,
          cast(a.u7m_id as string) as u7m_id,
         	'SKE_DebtLoan0' as key,
          cast(null as string) as value_c,
         	cast(coalesce(a.SKE_DebtLoan0, 0) as decimal(16,4)) as value_n,
          cast(null as timestamp) as value_d
      from
         	$Node3t_team_k7m_aux_d_scoreIN a
         	join $Node2t_team_k7m_aux_d_scoreIN b on (a.u7m_id = b.u7m_id)
      where
        	a.ske_debtloan0 is not NULL
    """
    )
    createHiveTableStage2_4
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage2_5 = spark.sql(
      s"""select
         	cast(a.exec_id as string) as exec_id,
          cast(null as string) as okr_id,
          cast(a.u7m_id as string) as u7m_id,
         	'SKE_DebtLeasing0' as key,
          cast(null as string) as value_c,
         	cast(coalesce(a.SKE_DebtLeasing0, 0) as decimal(16,4)) as value_n,
          cast(null as timestamp) as value_d
          from
         	$Node3t_team_k7m_aux_d_scoreIN a
         	join $Node2t_team_k7m_aux_d_scoreIN b on (a.u7m_id = b.u7m_id)
      where
        	a.ske_debtleasing0 is not NULL
    """
    )
    createHiveTableStage2_5
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage2_6 = spark.sql(
    s"""select
         	cast(a.exec_id as string) as exec_id,
          cast(null as string) as okr_id,
          cast(a.u7m_id as string) as u7m_id,
         	'SKE_DebtLoanTransact' as key,
          cast(null as string) as value_c,
         	cast(coalesce(a.ske_dloans, 0) as decimal(16,4)) as value_n,
          cast(null as timestamp) as value_d
     from
         	$Node3t_team_k7m_aux_d_scoreIN a
         	join $Node2t_team_k7m_aux_d_scoreIN b on (a.u7m_id = b.u7m_id)
     where
        	a.ske_dloans is not NULL
    """
  )
    createHiveTableStage2_6
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage2_7 = spark.sql(
      s"""select
         	cast(a.exec_id as string) as exec_id,
          cast(null as string) as okr_id,
          cast(a.u7m_id as string) as u7m_id,
         	'SKE_DebtLeasingTransact' as key,
          cast(null as string) as value_c,
         	cast(coalesce(a.ske_dlising, 0) as decimal(16,4)) as value_n,
          cast(null as timestamp) as value_d
      from
         	$Node3t_team_k7m_aux_d_scoreIN a
         	join $Node2t_team_k7m_aux_d_scoreIN b on (a.u7m_id = b.u7m_id)
       where
          a.ske_dlising is not NULL
    """
    )
    createHiveTableStage2_7
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг4 - добавление risk_segment
    val createHiveTableStage4 = spark.sql(
      s"""
         select
                 src.exec_id as exec_id,
                 src.okr_id  as okr_id,
                 src.u7m_id  as u7m_id,
                 'RISK_SEGMENT_OFFLINE' as key,
                 rs.RISK_SEGMENT as value_c,
                 cast(null as decimal(16, 4)) as value_n,
                 cast(null as timestamp) as value_d
         from
                 $Node2t_team_k7m_aux_d_scoreIN src
                 left join $Node4t_team_k7m_aux_d_scoreIN rs on (src.u7m_id = rs.u7m_id)
          where
         		rs.risk_segment_offline is not NULL
    """
    )
    createHiveTableStage4
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг5 - добавление параметров te
    val createHiveTableStage5 = spark.sql(
      s"""
         select
                 src.exec_id,
                 src.okr_id,
                 src.u7m_id,
                 'PTE' as key,
                 cast(null as string) as value_c,
                 cast(ckp.pte_val as decimal(16, 4)) as value_n,
                 cast(null as timestamp) as value_d
         from
                 $Node2t_team_k7m_aux_d_scoreIN src --score_keys
                 join $Node5t_team_k7m_aux_d_scoreIN clu on (clu.u7m_id = src.u7m_id) --clu
                 join $Node6t_team_k7m_aux_d_scoreIN ckp on (clu.reg_country = ckp.country_cd) --amr_country_ckp
         where
                 coalesce(lgte_val, -1) between 0 and 1 and
                 coalesce(pte_val, -1) between 0 and 1
         union all
         select
                 src.exec_id,
                 src.okr_id,
                 src.u7m_id,
                 'LGTE' as key,
                 cast(null as string) as value_c,
                 cast(ckp.lgte_val as decimal(16, 4)) as value_n,
                 cast(null as timestamp) as value_d
         from
                 $Node2t_team_k7m_aux_d_scoreIN src --score_keys
                 join $Node5t_team_k7m_aux_d_scoreIN clu on (clu.u7m_id = src.u7m_id) --clu
                 join $Node6t_team_k7m_aux_d_scoreIN ckp on (clu.reg_country = ckp.country_cd) --amr_country_ckp
         where
                 coalesce(lgte_val, -1) between 0 and 1 and
                 coalesce(pte_val, -1) between 0 and 1
    """
    )
    createHiveTableStage5
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг6 - добавление RAROC
    val createHiveTableStage6 = spark.sql(
      s"""
         select
                 src.exec_id,
                 src.okr_id,
                 src.u7m_id,
                 'K_RWA' as key,
                 cast(null as string) as value_c,
                 cast(1 as decimal(16, 4)) as value_n,
                 cast(null as timestamp) as value_d
         from
                 $Node2t_team_k7m_aux_d_scoreIN src --score_keys
    """
    )
    createHiveTableStage6
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг7 - добавление показателей ProxyLGD (LGD_OFFLINE)
    val createHiveTableStage7_1 = spark.sql(
      s"""
         SELECT
         		src.exec_id,
         		src.okr_id,
         		src.u7m_id,
         		'acrecbc_2_qa' as key,
         		cast(null as string) as value_c,
         		CAST(a.acrecbc_2_qa as DECIMAL(16,4)) as value_n,
            cast(null as timestamp) as value_d
           FROM
         		$Node2t_team_k7m_aux_d_scoreIN  src
         		join $Node7t_team_k7m_aux_d_scoreIN a on a.u7m_id = src.u7m_id
         		where a.acrecbc_2_qa is not NULL
    """
    )
    createHiveTableStage7_1
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage7_2 = spark.sql(
      s"""
         SELECT
         		src.exec_id,
         		src.okr_id,
         		src.u7m_id,
         		'brwisyng_3_sm' as key,
         		cast(null as string) as value_c,
         		CAST(a.brwisyng_3_sm as DECIMAL(16,4)) as value_n,
            cast(null as timestamp) as value_d
           FROM
         		$Node2t_team_k7m_aux_d_scoreIN  src
         		join $Node7t_team_k7m_aux_d_scoreIN a on a.u7m_id = src.u7m_id
         		where a.brwisyng_3_sm is not NULL
    """
    )
    createHiveTableStage7_2
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage7_3 = spark.sql(
      s"""
         SELECT
         		src.exec_id,
         		src.okr_id,
         		src.u7m_id,
         		'cas2eqr_2_qa' as key,
         		cast(null as string) as value_c,
         		CAST(a.cas2eqr_2_qa as DECIMAL(16,4)) as value_n,
         		cast(null as timestamp) as value_d
           FROM
         		$Node2t_team_k7m_aux_d_scoreIN src
         		join $Node7t_team_k7m_aux_d_scoreIN a on a.u7m_id = src.u7m_id
         		where	a.cas2eqr_2_qa is not NULL
    """
    )
    createHiveTableStage7_3
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage7_4 = spark.sql(
      s"""
         SELECT
         		src.exec_id,
         		src.okr_id,
         		src.u7m_id,
         		'curliqr_2' as key,
         		cast(null as string) as value_c,
         		CAST(a.curliqr_2 as DECIMAL(16,4)) as value_n,
         		cast(null as timestamp) as value_d
           FROM
         		$Node2t_team_k7m_aux_d_scoreIN src
             		join $Node7t_team_k7m_aux_d_scoreIN a on a.u7m_id = src.u7m_id
            where  a.curliqr_2 is not null
    """
    )
    createHiveTableStage7_4
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage7_5 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'noncur_assets_y' as key,
         	cast(null as string) as value_c,
         	CAST(MAX(a.noncur_assets_y) as DECIMAL(16,4)) as value_n,
         	cast(null as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
         		join $Node9t_team_k7m_aux_d_scoreIN a on a.u7m_id = src.u7m_id
         		where  a.noncur_assets_y is not null
         group by src.exec_id, src.okr_id, src.u7m_id
    """
    )
    createHiveTableStage7_5
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage7_6 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'dummy_opkflag' as key,
         	cast(null as string) as value_c,
         	CAST(a.dummy_opkflag as DECIMAL(16,4)) as value_n,
         	cast(null as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
         		join $Node7t_team_k7m_aux_d_scoreIN a on a.u7m_id = src.u7m_id
         		where  a.dummy_opkflag is not null
    """
    )
    createHiveTableStage7_6
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage7_7 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'rexp2dbt_QA' as key,
         	cast(null as string) as value_c,
         	CAST(a.rexp2dbt_QA as DECIMAL(16,4)) as value_n,
         	cast(null as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
         		join $Node7t_team_k7m_aux_d_scoreIN a on a.u7m_id = src.u7m_id
         		where  a.rexp2dbt_QA is not null
    """
    )
    createHiveTableStage7_7
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage7_8 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'rg2avsbr_sm2_QA' as key,
         	cast(null as string) as value_c,
         	CAST(a.rg2avsbr_sm2_QA as DECIMAL(16,4)) as value_n,
         	cast(null as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
         		join $Node7t_team_k7m_aux_d_scoreIN a on a.u7m_id = src.u7m_id
         		where  a.rg2avsbr_sm2_QA is not null
    """
    )
    createHiveTableStage7_8
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage7_9 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'stl2as_2_QA' as key,
         	cast(null as string) as value_c,
         	CAST(a.stl2as_2_QA as DECIMAL(16,4)) as value_n,
         	cast(null as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
         		join $Node7t_team_k7m_aux_d_scoreIN a on a.u7m_id = src.u7m_id
         		where  a.stl2as_2_QA is not null
    """
    )
    createHiveTableStage7_9
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    val createHiveTableStage7_10 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'cr2as_2_QA' as key,
         	cast(null as string) as value_c,
         	CAST(a.cr2as_2_QA as DECIMAL(16,4)) as value_n,
         	cast(null as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
         		join $Node7t_team_k7m_aux_d_scoreIN a on a.u7m_id = src.u7m_id
         		where  a.cr2as_2_QA is not null
    """
    )
    createHiveTableStage7_10
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг8 - добавление PNPO_FLAG
    val createHiveTableStage8 = spark.sql(
      s"""
        SELECT
            src.exec_id,
            src.okr_id,
            src.u7m_id,
            'PNPO_FLAG' as key,
            cast(null as string) as value_c,
            CAST(if(ods.OVERP_FLG = 1, 1, 0) as DECIMAL(16,4)) as value_n,
            cast(null as timestamp) as value_d
          FROM
         	  $Node2t_team_k7m_aux_d_scoreIN src
           join $Node5t_team_k7m_aux_d_scoreIN clu on (src.u7m_id = clu.u7m_id)
           join $Node12t_team_k7m_aux_d_scoreIN ods on (clu.crm_id = ods.crm_id)
         WHERE
           ods.OVERP_FLG is not null
    """
    )
    createHiveTableStage8
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг9 - добавление FLAG_OVER_EXIST
    val createHiveTableStage9 = spark.sql(
      s"""
          SELECT
                  src.exec_id,
                  src.okr_id,
                  src.u7m_id,
                  'FLAG_OVER_EXIST' as key,
                  cast(null as string) as value_c,
                  CAST(if(ods.overe_flg = 1, 1, 0) as DECIMAL(16,4)) as value_n,
                  cast(null as timestamp) as value_d
            FROM
                  $Node2t_team_k7m_aux_d_scoreIN src
                   join $Node5t_team_k7m_aux_d_scoreIN clu on (src.u7m_id = clu.u7m_id)
                   join $Node10t_team_k7m_aux_d_scoreIN ods on (clu.eks_id = cast(cast(ods.eks_id as bigint) as string))
           WHERE
                   ods.overe_flg is not null
    """
    )
    createHiveTableStage9
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг10 - добавление MAX_TRANS_DATE
    val createHiveTableStage10 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'MAX_TRANS_DATE' as key,
         	cast(null as string) as value_c,
         	CAST(null as DECIMAL(16,4)) as value_n,
          CAST(add_months(q.value_d, 3) as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
    cross join (select max(run_dt) value_d from $Node8t_team_k7m_aux_d_scoreIN s
    where s.status='RUNNING')  q
    """
    )
    createHiveTableStage10
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг11 - добавление MIN_TRANS_DATE
    val createHiveTableStage11 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'MIN_TRANS_DATE' as key,
         	cast(null as string) as value_c,
         	CAST(null as DECIMAL(16,4)) as value_n,
          CAST(coalesce(ods.overe_dt, q.value_d) as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
          join $Node5t_team_k7m_aux_d_scoreIN clu on (src.u7m_id = clu.u7m_id)
          cross join (select max(run_dt) value_d from $Node8t_team_k7m_aux_d_scoreIN s
            where s.status='RUNNING')  q
          left join $Node10t_team_k7m_aux_d_scoreIN ods on (clu.eks_id = cast(cast(ods.eks_id as bigint) as string))
    """
    )
    createHiveTableStage11
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг12 - добавление EKS_BURROVER_RATING
    val createHiveTableStage12 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'EKS_BURROVER_RATING' as key,
         	cast(ovr.c_raiting as string) as value_c,
         	CAST(null as DECIMAL(16,4)) as value_n,
          CAST(null as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
          join $Node5t_team_k7m_aux_d_scoreIN clu on (src.u7m_id = clu.u7m_id)
          join (
           select
                   c_client,
                   cast(c_raiting as int) as c_raiting,
                   c_proc,
                   row_number() over (partition by c_client order by c_date_calc desc) rn
             from
                   $Node11t_team_k7m_aux_d_scoreIN
          ) ovr on (clu.eks_id = cast(cast(ovr.c_client as bigint) as string) and ovr.rn = 1)
         WHERE
          ovr.c_raiting is not null
    """
    )
    createHiveTableStage12
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг13 - добавление OVER_TURN_K
    val createHiveTableStage13 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'OVER_TURN_K' as key,
         	cast(null as string) as value_c,
         	CAST(ovr.c_proc as DECIMAL(16,4)) as value_n,
          CAST(null as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
          join $Node5t_team_k7m_aux_d_scoreIN clu on (src.u7m_id = clu.u7m_id)
          join (
           select
                   c_client,
                   cast(c_raiting as int) as c_raiting,
                   c_proc,
                   row_number() over (partition by c_client order by c_date_calc desc) rn
             from
                   $Node11t_team_k7m_aux_d_scoreIN
          ) ovr on (clu.eks_id = cast(cast(ovr.c_client as bigint) as string) and ovr.rn = 1)
         WHERE
          ovr.c_proc is not null
    """
    )
    createHiveTableStage13
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    //шаг14 - добавление OVER_CAP
    val createHiveTableStage14 = spark.sql(
      s"""
        SELECT
         	src.exec_id,
         	src.okr_id,
         	src.u7m_id,
         	'OVER_CAP' as key,
         	cast(null as string) as value_c,
         	CAST(ovr.c_over_limit_res as DECIMAL(16,4)) as value_n,
          CAST(null as timestamp) as value_d
         FROM
         	$Node2t_team_k7m_aux_d_scoreIN src
          join $Node5t_team_k7m_aux_d_scoreIN clu on (src.u7m_id = clu.u7m_id)
          join (
           select
                   c_client,
                   cast(c_raiting as int) as c_raiting,
                   c_proc,
                   c_over_limit_res,
                   row_number() over (partition by c_client order by c_date_calc desc) rn
             from
                   $Node11t_team_k7m_aux_d_scoreIN
          ) ovr on (clu.eks_id = cast(cast(ovr.c_client as bigint) as string) and ovr.rn = 1)
         WHERE
          ovr.c_over_limit_res is not null
    """
    )
    createHiveTableStage14
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_scoreOUT")

    logInserted()
    logEnd()
  }
}

