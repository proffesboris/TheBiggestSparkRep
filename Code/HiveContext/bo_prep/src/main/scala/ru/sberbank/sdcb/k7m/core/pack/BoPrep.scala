package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{SaveMode, SparkSession}

class BoPrep(override val spark: SparkSession, val config: Config) extends EtlJob with EtlLogger {

  val sourcePath = "/user/team/team_smart/input_files/best_offers"



  override val processName = "BO_PREP"
  var dashboardName = s"${config.pa}.bo"

  val csvOptions = Map(
    "sep" -> ";",
    "header" -> "true",
    "escapeQuotes" -> "false",
    "quote" -> "",
    "dateFormat" -> "dd.MM.yyyy",
    "timestampFormat" -> "dd.MM.yyyy",
    "comment" -> "#")

  case class Table(schema: String, name: String, select: String) {
    override def toString: String = s"$schema.$name"
  }

  def common() = {
/*
    Seq("features", "clusters", "cubes").foreach { confName =>
      spark.read
        .options(csvOptions)
        .csv(s"$sourcePath/$confName.csv")
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(s"${config.aux}.bo_config_${confName}_src")
    }
*/
    spark.sql(s"drop table if exists ${config.aux}.dict_bo_features")

    dashboardName = s"${config.aux}.dict_bo_features"

    spark.sql(s"""create table ${config.aux}.dict_bo_features as
                |select
                |cast(feature_id as bigint) as feature_id
                |,code as  feature_cd
                |,name as name
                |--,delete as delete_desc
                |--,slice as  slice_desc
                |,func as func_desc
                |,cast(step as double) as step_cnt
                |,type as type_name
                |,comment as comment_desc
                |,eks_fld as eks_fld
                | FROM ${config.aux}.bo_config_features_src
                | where cast(feature_id as bigint) is not null""".stripMargin)

    logInserted()

    spark.sql(s"drop table if exists ${config.aux}.dict_bo_clusters")

    dashboardName = s"${config.aux}.dict_bo_clusters"

    spark.sql(s"""create table ${config.aux}.dict_bo_clusters as
                |select 
                |cast(row_number() over (order by name) as bigint) as clust_id,
                |name
                |FROM ( SELECT distinct clust_id as name FROM ${config.aux}.bo_config_clusters_src) T""".stripMargin)
    logInserted()

    spark.sql(s"drop table if exists ${config.aux}.dict_BO_cube2cluster")

    dashboardName = s"${config.aux}.dict_BO_cube2cluster"

    spark.sql(s"""create table ${config.aux}.dict_BO_cube2cluster as 
                |SELECT  
                |cast(c.bo_id as bigint) as cube_id 
                |,d.clust_id as clust_id
                |,cast(c.bo_quality as decimal(16,5)) as bo_quality
                |FROM ${config.aux}.bo_config_clusters_src C,
                |${config.aux}.dict_bo_clusters D
                |where c.clust_id=d.name""".stripMargin)
    logInserted()

    spark.sql(s"drop table if exists ${config.aux}.dict_bo_cubes")

    dashboardName = s"${config.aux}.dict_bo_cubes"

    spark.sql(s"""create table ${config.aux}.dict_bo_cubes as 
                |SELECT 
                |cast(c.bo_id as bigint) as cube_ID
                |,f.feature_id
                |,cast(c.min as decimal(16,5)) as min_val
                |,cast(c.max as decimal(16,5)) as max_val
                |,case when c.categorial='nan' or trim(c.categorial)='' then null else c.categorial end as categorial_val
                |FROM ${config.aux}.bo_config_cubes_src C,
                |${config.aux}.dict_bo_features F
                |WHERE c.feature_id=f.feature_cd""".stripMargin)
    logInserted()

    spark.sql(s"drop table if exists ${config.aux}.dict_BO_PD_BY_SCALE")

    dashboardName = s"${config.aux}.dict_BO_PD_BY_SCALE"

    spark.sql(s"""create table ${config.aux}.dict_BO_PD_BY_SCALE (
                |SBER_RAITING TINYINT
                |,PD DOUBLE
                |,Lower_Bound DOUBLE
                |,Upper_Bound DOUBLE)""".stripMargin)
    
    spark.sql(s"""INSERT INTO ${config.aux}.dict_BO_PD_BY_SCALE  
                |SELECT 1,0.0002,0,0.00032 UNION ALL
                |SELECT 2,0.00037,0.00032,0.00044 UNION ALL
                |SELECT 3,0.00051,0.00044,0.0006 UNION ALL
                |SELECT 4,0.0007,0.0006,0.00083 UNION ALL
                |SELECT 5,0.00097,0.00083,0.00114 UNION ALL
                |SELECT 6,0.00133,0.00114,0.00156 UNION ALL
                |SELECT 7,0.00184,0.00156,0.00215 UNION ALL
                |SELECT 8,0.00253,0.00215,0.00297 UNION ALL
                |SELECT 9,0.00348,0.00297,0.00409 UNION ALL
                |SELECT 10,0.0048,0.00409,0.00563 UNION ALL
                |SELECT 11,0.0066,0.00563,0.00775 UNION ALL
                |SELECT 12,0.0091,0.00775,0.01067 UNION ALL
                |SELECT 13,0.01253,0.01067,0.0147 UNION ALL
                |SELECT 14,0.01725,0.0147,0.02024 UNION ALL
                |SELECT 15,0.02375,0.02024,0.02788 UNION ALL
                |SELECT 16,0.03271,0.02788,0.03839 UNION ALL
                |SELECT 17,0.04505,0.03839,0.05287 UNION ALL
                |SELECT 18,0.06204,0.05287,0.0728 UNION ALL
                |SELECT 19,0.08543,0.0728,0.10026 UNION ALL
                |SELECT 20,0.11765,0.10026,0.13807 UNION ALL
                |SELECT 21,0.16203,0.13807,0.19014 UNION ALL
                |SELECT 22,0.22313,0.19014,0.26185 UNION ALL
                |SELECT 23,0.30728,0.26185,0.36059  UNION ALL
                |SELECT 24,0.42316,0.36059,0.49659 UNION ALL
                |SELECT 25,0.58275,0.49659,1 UNION ALL
                |SELECT 26,1,1,NULL""".stripMargin)
    logInserted()
  }

  //---------------------------------------------------------
  def prep() = {

    spark.sql(s"drop table if exists ${config.aux}.bo_prep")

    dashboardName = s"${config.aux}.bo_prep"

    spark.sql(s"""create table ${config.aux}.bo_prep as
                |WITH
                |T_FOK00 AS ( --ФОК отчетность 
                |select crm_cust_id
                |,fin_stmt_year as y
                |,fin_stmt_period as p
                |--Формируем дату отчетности из года и квартала
                |,ADD_MONTHS( -- Добавляем 1 месяц, чтобы получить отчетную дату (конец квартала+1)
                |CAST(
                |SUBSTR(
                |from_unixtime( --Преобразование unix_date в дату
                |unix_timestamp( --Преобразование в unix_date по формату
                |CONCAT(cast(cast(fin_stmt_year as int) as string),'/',cast(cast(fin_stmt_period*3 as int) as string),'/01') --Строка даты , Period*3 - последний месяц отчетного квартала/года, первое число
                |,'yyyy/M/d') 
                |),1,10)
                |as date)
                |,1) as rep_date 
                |, Fin_stmt_2110_amt
                |FROM ${config.aux}.fok_fin_stmt_rsbu
                |--WHERE CRM_CUST_ID='1-Q5E0X' --для проверки на одном клиенте
                |),
                |T_FOK0 AS ( -- расчет выручки , скользящее окно за год
                |SELECT 
                | b.crm_cust_id
                | ,b.rep_date
                | ,(coalesce(b.fin_stmt_2110_amt, 0) 
                | + coalesce(fc.fin_stmt_2110_amt, 0)
                | - coalesce(c.fin_stmt_2110_amt, 0)) 
                | as rev_y 
                | FROM T_FOK00 b 
                | INNER JOIN T_FOK00 c ON --Переделал с LEFT JOIN на INNER т.к. иначе будут некорректные цифры в случае отсутствия предыдущей отчетности
                |( (b.y - 1) = c.y  AND b.p = c.p AND b.crm_cust_id = c.crm_cust_id) --Прошлый год, тот же квартал
                | INNER JOIN T_FOK00 fc on --Переделал с LEFT JOIN на INNER т.к. иначе будут некорректные цифры в случае отсутствия предыдущей отчетности
                |( (b.y - 1) = fc.y AND fc.p = 4 AND b.crm_cust_id = fc.crm_cust_id) --Прошлый год 4 квартал (за год)
                |--WHERE (c.crm_cust_id is not null and fc.crm_cust_id is not null) or b.p=4 -- расскомментировать и сделать Inner вместо LEFT выше, если все-таки признают расчеты с нехваткой данных по периодам некорректными
                |),
                |T_FOK as (--Отчетность ФОК + отсечка по базису
                |select 
                | b.u7m_id as U7M_id
                |--, b.crm_id as CRM_id
                |--, b.inn as INN
                |, rep_date
                |, rev_y
                | from ${config.pa}.CLU b --отсечка по базису
                | ,T_FOK0 F
                |WHERE b.crm_id=f.crm_cust_id
                |and B.flag_basis_client='Y'
                |--and (REP_DATE>(DATE_SUB(CURRENT_DATE,36*30))) --Отчетность не старше 36 мес.
                |--and Fin_stmt_2110_amt is not null
                |),
                |T_INT as (--Отчетность Интегрум + отсечка по базису
                |select 
                | b.u7m_id as U7M_id
                |--, b.crm_id as crm_id
                |--, b.inn as INN
                |,add_months(CAST(fin_stmt_start_dt as date),12) AS REP_DATE --В исходной таблице строка, обещали переделать, пока заглушка (20180313), по неизвестной причине дата начала периода, прибавляем 12 мес. чтобы получить отчетную дату
                |, Fin_stmt_2110_amt as rev_y
                | from ${config.pa}.CLU b --отсечка по базису
                |  , ${config.aux}.INT_fin_stmt_rsbu i
                |WHERE b.inn=i.cust_inn_num
                | and B.flag_basis_client='Y'
                | --and (CAST(fin_stmt_start_dt as DATE)>(DATE_SUB(CURRENT_DATE,36*30))) --Отчетность не старше 36 мес.
                |-- and Fin_stmt_2110_amt is not null
                |),
                |MD_FOK as (--Максимальная дата ФОК
                |SELECT u7m_id,max(rep_date) as max_rep_date FROM T_FOK GROUP BY u7m_id
                |),
                |MD_INT as (--Максимальная дата Интегрум
                |SELECT u7m_id,max(rep_date) as max_rep_date FROM T_INT GROUP BY u7m_id
                |),
                |T_SRC as (--Выбор отчетности между ФОК и Интегрум
                |SELECT 
                |COALESCE(F.u7m_id,I.u7m_id) as u7m_id
                |--,COALESCE(F.INN,I.inn) as inn
                |,CASE 
                | WHEN F.MAX_REP_DATE IS NOT NULL AND I.MAX_REP_DATE is NOT NULL   --Есть отчетность ФОК и Интегрум, выбираем Макс. дату
                |    THEN 
                |      CASE WHEN F.MAX_REP_DATE>=I.MAX_REP_DATE THEN 'FOK' ELSE 'INT' END --Выбираем ФОК или Интегрум по макс. дате
                | WHEN F.MAX_REP_DATE IS NOT NULL AND I.MAX_REP_DATE is NULL  --Есть только отчетность ФОК
                |    THEN 
                |      'FOK'
                | WHEN F.MAX_REP_DATE IS NULL AND I.MAX_REP_DATE is NOT NULL  --Есть только отчетность Интегрум
                |    THEN 
                |      'INT'
                | ELSE
                |      NULL
                |END as SRC,
                |CASE 
                | WHEN F.MAX_REP_DATE IS NOT NULL AND I.MAX_REP_DATE is NOT NULL   --Есть отчетность ФОК и Интегрум, выбираем Макс. дату
                |    THEN 
                |      CASE WHEN F.MAX_REP_DATE>=I.MAX_REP_DATE THEN f.max_rep_date ELSE i.max_rep_date END --Выбираем ФОК или Интегрум по макс. дате
                | WHEN F.MAX_REP_DATE IS NOT NULL AND I.MAX_REP_DATE is NULL  --Есть только отчетность ФОК
                |    THEN 
                |      f.max_rep_date
                | WHEN F.MAX_REP_DATE IS NULL AND I.MAX_REP_DATE is NOT NULL  --Есть только отчетность Интегрум
                |    THEN 
                |      i.max_rep_date
                |  ELSE
                |      NULL
                |END as MAX_REP_DATE
                |FROM MD_FOK F
                |FULL JOIN MD_INT I on f.u7m_id=i.u7m_id
                |)
                |--Сборка конечной таблицы
                |SELECT
                |T_FOK.u7m_id as u7m_id
                |--,T_FOK.crm_id as crm_id
                |--,T_FOK.inn as inn
                |,T_FOK.rep_date as rep_date
                |,T_FOK.rev_y*1000 as rev_y
                |,t_src.src as SRC
                |FROM T_FOK JOIN T_SRC on 
                |T_SRC.SRC='FOK' AND T_SRC.u7m_id=T_FOK.u7m_id  AND T_SRC.MAX_REP_DATE=T_FOK.REP_DATE
                |UNION ALL
                |SELECT
                |T_INT.u7m_id as u7m_id
                |--,T_INT.crm_id as crm_id
                |--,T_INT.inn as inn
                |,T_INT.rep_date as rep_date
                |,T_INT.rev_y*1000 as rev_y
                |,t_src.src as SRC
                |FROM T_INT JOIN T_SRC on 
                |T_SRC.SRC='INT' AND T_SRC.u7m_id=T_INT.u7m_id  AND T_SRC.MAX_REP_DATE=T_INT.REP_DATE""".stripMargin)
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("parquet")
//      .option("path", s"${config.auxPath}bo_prep")
//      .saveAsTable(s" ${config.aux}.bo_prep")

    logInserted()
  }

  def run() {

    logStart()

    common()
    //commonCheck()
    prep()

    logEnd()
  }
}