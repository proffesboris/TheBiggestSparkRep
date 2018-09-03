package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{SaveMode, SparkSession}

class ProxyLGDIn(override val spark: SparkSession, val config: Config) extends EtlJob with EtlLogger {

  val stgSchema = config.stg
  val targetAuxSchema = config.aux
  val targetPaSchema = config.pa

  val tblBoLGD = s"${config.aux}.bo_lgd"



  //---------------------------------------------------------

  case class Table(schema: String, name: String, select: String) {
    override def toString: String = s"$schema.$name"
  }

  override val processName = "ProxyLGDin"
  var dashboardName = s"${config.pa}.ProxyLGDin"

  def run() {
    logStart()

    Seq(
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_01_00_COMMON",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_02_00_CUST_BASIS",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_03_00_CRM_DEBT",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_04_00_BO_EXP_U",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_05_00_FOK_ALL",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_06_00_FOK_CHECK",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_07_00_FOK_MAXDATE",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_08_00_INT_ALL",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_09_00_INT_CHECK",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_10_00_INT_MAXDATE",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_11_00_REP_FIN",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_12_00_REP_SRC",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_13_00_CALC_SRC",
      s"DROP TABLE IF EXISTS ${config.aux}.ProxyLGDin_STG_14_00_CALC_CUST",
      s"DROP TABLE IF EXISTS ${config.pa}.ProxyLGDin"
    ).foreach(spark.sql(_))

    dashboardName = s"${config.aux}.ProxyLGDin_STG_01_00_COMMON"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_01_00_COMMON as
                |With T_AGE as (
                |select param_val
                |,(CAST(CONCAT(SUBSTR(start_dt,7,4),'-',SUBSTR(start_dt,4,2),'-',SUBSTR(start_dt,1,2)) as date)) as D1
                |,(CAST(CONCAT(SUBSTR(end_dt,7,4),'-',SUBSTR(end_dt,4,2),'-',SUBSTR(end_dt,1,2)) as date)) as D2
                |from ${config.aux}.amr_lr_param where param_name='_middle_age'
                |), --АМРЛиРТ
                |T_ASSETS as (
                |select param_val
                |,(CAST(CONCAT(SUBSTR(start_dt,7,4),'-',SUBSTR(start_dt,4,2),'-',SUBSTR(start_dt,1,2)) as date)) as D1
                |,(CAST(CONCAT(SUBSTR(end_dt,7,4),'-',SUBSTR(end_dt,4,2),'-',SUBSTR(end_dt,1,2)) as date)) as D2
                |from ${config.aux}.amr_lr_param where param_name='_middle_assets'
                |), --АМРЛиРТ
                |T_CONST as (
                |SELECT
                |  CAST(current_date() as date) AS CALC_DATE --Дата расчета витрины, возможно будет храниться для всех витрин в виде показателя
                |, CAST(DATE_SUB(CURRENT_date(),DAY(CURRENT_date())-1) as date)  AS month_first_day --использует дату расчета витрины
                |, cast(0 AS TinyINT) AS ProxyLGDin_CONST_ANetCCO_to_EAD
                |, cast(0 AS TinyINT) AS ProxyLGDin_CONST_collosn5
                |, cast(0 AS TinyINT) AS ProxyLGDin_CONST_cnedvokr_nd
                |, cast(1 AS TinyINT) AS ProxyLGDin_CONST_brtclwner3_ok_1_nd
                |, cast(0 AS TinyINT) AS ProxyLGDin_CONST_maxcolgivrate_osn_1_nd
                |, cast(10000000000000000 AS BigINT) AS ProxyLGDin_CONST_INFINITY
                |, cast(NULL AS decimal(16,5)) AS ProxyLGDin_CONST_NULL
                |, cast(0 AS TinyINT) AS ProxyLGDin_CONST_ufn_flag
                |, cast(21 AS INT) AS ProxyLGDin_CONST_REP_HIST --21 месяц
                |)
                |, T_AMR AS (
                |SELECT cast(T1.param_val as decimal(38,4)) AS middle_age,cast(T2.param_val as decimal(38,4)) AS middle_assets FROM T_CONST T0 CROSS JOIN T_AGE T1 CROSS JOIN T_ASSETS T2
                |WHERE
                |T0.CALC_DATE>=T1.D1 AND T0.CALC_DATE<T1.D2 --'_middle_age'
                |AND
                |T0.CALC_DATE>=T2.D1 AND T0.CALC_DATE<T2.D2 --'_middle_assets'
                |)
                |SELECT * FROM T_CONST CROSS JOIN T_AMR""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_02_00_CUST_BASIS"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_02_00_CUST_BASIS AS
                |select * from (
                |SELECT
                |c.u7m_id as u7m_id
                |,c.crm_id AS CRM_ID
                |,c.inn AS INN
                |,c.kpp AS KPP
                |--,COALESCE(org_reg_crm_dt,LEAST(nvl(org_reg_egrul_dt,CAST ('9999-12-31' as date)),NVL(org_reg_rosstat_dt,CAST ('9999-12-31' as date)))) AS RegistrationDate
                |,cast(
                |(COALESCE(
                |cast(b.org_reg_crm_dt as date),
                |LEAST( nvl(cast(b.org_reg_egrul_dt as date),CAST ('9999-12-31' as date)), NVL(cast(b.org_reg_rosstat_dt as date),CAST ('9999-12-31' as date)))
                |)
                |) as date)
                | AS RegistrationDate
                |,COALESCE(b.risk_segment_name,PR.risk_segment_offline) AS Risk_Segment --Если риск-сегмент определен Базисе - используем его, иначе берем из модели
                |,org_segment_name AS attrib_03
                |FROM  ${config.pa}.CLU c --CLU основа, U7M_ID
                |LEFT JOIN ${config.aux}.BASIS_CLIENT b --Риск сегмент и др. из базиса
                | ON c.crm_id=b.org_crm_id
                |LEFT join ${config.pa}.proxyrisksegmentstandalone PR --Модельный риск-сегмент
                | ON c.u7m_id=pr.u7m_id
                |AND c.flag_basis_client='Y'
                |) T
                |WHERE Risk_Segment IN ('Клиенты, опер.д-ть меньше 5 кв','Контрактное кредитование','Корпор. компания-резидент (РФ)', 'Кредитование по пилот. моделям') --Фильтр по риск-сегменту""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_03_00_CRM_DEBT"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_03_00_CRM_DEBT AS
                |With
                | T_CUR as ( --Курсы валют на дату расчета LGD
                | SELECT T3.* FROM (select  T2.c_cur_short as CUR, T2.course as V,T2.from_dt as D1,T2.to_dt as D2, ROW_NUMBER() over (partition by c_cur_short order by from_dt desc) AS RN from 
                | ${config.aux}.ProxyLGDin_STG_01_00_COMMON T1 CROSS JOIN 
                | ${config.aux}.k7m_cur_courses T2 where T1.CALC_DATE between T2.from_dt and T2.to_dt
                | ) T3 WHERE T3.RN=1),--Отсечка дублей (DEM)
                |crm_cust as (select distinct org.row_id as cust_id--,org_x.sbrf_inn as inn,org.name as ORG_NAME
                |   FROM ${config.stg}.CRM_S_ORG_EXT  org
                |   LEFT JOIN ${config.stg}.CRM_S_ORG_EXT_X  org_x  on org.ROW_ID = org_x.PAR_ROW_ID
                |   where org.INT_ORG_FLG='N' and  lower(org.OU_TYPE_CD)<>'территория'  --and org.row_id IN ('1-1XVAK4X','1-1YLRKG6','1-B7M0WS')
                |   --LIMIT 1
                |   ),
                |REVN as (
                |SELECT T5.row_id,T5.x_curr_dbt*T_CUR.V as x_curr_dbt,T5.X_ASSD_WRNG_DAT_FLG,T5.x_agr_id,T5.REVN_STAT_CD,T5.CUR FROM 
                |(SELECT T4.row_id,T4.x_curr_dbt,T4.X_ASSD_WRNG_DAT_FLG,T4.x_agr_id,T4.REVN_STAT_CD,T4.revn_amt_curcy_cd as CUR
                |,row_number() over (partition by T4.row_id order by T4.CTL_ValidFrom DESC,T4.ods_sequenceid desc) as RN
                |FROM ${config.stg}.CRM_S_REVN T4
                |where T4.X_RKL_MAIN_FLG <> 'Y'    --20/07/2018 изменение по отсечению поручителей
                |and (T4.TYPE_CD <> 'Loan Offer')  --20/07/2018 изменение по отсечению поручителей
                |) T5
                |INNER JOIN T_CUR on T5.CUR=T_CUR.CUR -- курсы валют
                |AND T5.RN=1
                |),
                |AGREE_X as (
                |SELECT T7.ROW_ID,T7.SBRFAgreementDebt*T_CUR.V as SBRFAgreementDebt,T7.CUR FROM (
                |SELECT T6.ROW_ID,(T6.X_DEBT_REMINDER+T6.X_OVERDUE_DEBT_REMINDER)/1000 as SBRFAgreementDebt,T6.x_currency_code as CUR
                |,ROW_NUMBER() over (partition by T6.ROW_ID order by T6.CTL_validfrom desc,T6.ods_sequenceid desc) AS RN
                | FROM ${config.stg}.CRM_cx_agree_x T6) T7
                |INNER JOIN T_CUR on T7.CUR=T_CUR.CUR -- курсы валют
                |and T7.RN=1
                | )
                |SELECT b.U7M_ID as U7M_ID,P.CUST_ID as CRM_ID,
                |SUM(
                |CASE p.X_ASSD_WRNG_DAT_FLG
                |  WHEN 'Y' THEN NVL(p.SBRFCurrentDebt,p.SBRFAgreementDebt)
                |  WHEN 'N' THEN p.SBRFAgreementDebt
                |ELSE NULL END) as CRM_DEBT FROM
                |${config.pa}.CLU B --По всем клиентам базиса
                |LEFT JOIN 
                |(select c.*,revn.X_ASSD_WRNG_DAT_FLG,rEVN.x_curr_dbt as SBRFCurrentDebt,agree_x.SBRFAgreementDebt as SBRFAgreementDebt--,rEVN.row_id,rEVN.x_agr_id,AGREE_X.ROW_ID
                |from crm_cust c
                |join ${config.stg}.CRM_S_OPTYPRD_ORG o on o.OU_ID = c.cust_id
                |join REVN on o.OPTY_PRD_ID = REVN.row_id and REVN.REVN_STAT_CD in ('Действующий','В процессе изменения')
                |LEFT JOIN AGREE_X on REVN.x_agr_id=AGREE_X.row_ID
                |where o.status_cd is null    --20/07/2018 изменение по отсечению поручителей
                |) P on P.CUST_ID =b.crm_id 
                |WHERE b.flag_basis_client='Y' 
                |GROUP BY b.u7m_id,P.CUST_ID
                |""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_04_00_BO_EXP_U"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_04_00_BO_EXP_U AS
                 |SELECT
                 |b.BO_ID as BO_ID,
                 |cust.U7M_ID as U7M_ID,
                 |cust.crm_ID as CRM_ID,
                 |case
                 |   when c.CRM_DEBT is null     and b.value_n is null then null
                 |   when c.CRM_DEBT is not null and b.value_n is null then null
                 |   when c.cnt <> 0 and c.CRM_DEBT is null and b.value_n is not null then null  --20/07/2018 изменение по расчету задолженностив том случае, если в CRM есть	действующие кредитные продукты и по ним нет задолженности
                 |   when c.CRM_DEBT is null     and b.value_n is not null then b.VALUE_N/1000               --действующих продуктов на клиенте нет совсем
                 |   when c.CRM_DEBT is not null and b.value_n is not null then (b.VALUE_N/1000) + c.CRM_DEBT
                 |   else NULL
                 |end AS EXP_SB
                 |FROM ${config.aux}.CLU_BASE as cust
                 |inner join ${config.aux}.bo_gen b on b.key='EXP_U' and b.U7M_ID = cust.u7m_id
                 |left join (select u7m_id, crm_id,
                 |                  count(crm_id) as cnt,
                 |                  sum(crm_debt) as crm_debt
                 |            from   ${config.aux}.ProxyLGDin_STG_03_00_CRM_DEBT
                 |            group by u7m_id, crm_id
                 |            ) c on c.u7m_id = cust.u7m_id
                 |where cust.flag_basis_client = 'Y'
                 |""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_05_00_FOK_ALL"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_05_00_FOK_ALL AS
                SELECT
                T2.U7M_ID as U7M_ID
                |--,T2.CRM_ID as CRM_ID
                |--,T2.INN AS INN
                |,
                |ADD_MONTHS( -- Добавляем 1 месяц, чтобы получить отчетную дату (конец квартала+1)
                |CAST(
                |SUBSTR(
                |from_unixtime( --Преобразование unix_date в дату
                |unix_timestamp( --Преобразование в unix_date по формату
                |CONCAT(cast(cast(fin_stmt_year as int) as string),'/',cast(cast(fin_stmt_period*3 as int) as string),'/01') --Строка даты , Period*3 - последний месяц отчетного квартала/года, первое число
                |,'yyyy/M/d')
                |),1,10)
                |as date)
                |,1) as rep_date
                |--,fin_stmt_1100_amt AS BS1100,fin_stmt_1110_amt AS BS1110,fin_stmt_1120_amt AS BS1120,fin_stmt_1130_amt AS BS1130,fin_stmt_1140_amt AS BS1140,fin_stmt_1150_amt AS BS1150,fin_stmt_1160_amt AS BS1160,fin_stmt_1170_amt AS BS1170,fin_stmt_1180_amt AS BS1180,fin_stmt_1190_amt AS BS1190,fin_stmt_1200_amt AS BS1200,fin_stmt_1210_amt AS BS1210,fin_stmt_1220_amt AS BS1220,fin_stmt_1230_amt AS BS1230,fin_stmt_1240_amt AS BS1240,fin_stmt_1250_amt AS BS1250,fin_stmt_1260_amt AS BS1260,fin_stmt_1300_amt AS BS1300,fin_stmt_1310_amt AS BS1310,fin_stmt_1320_amt AS BS1320,fin_stmt_1340_amt AS BS1340,fin_stmt_1350_amt AS BS1350,fin_stmt_1360_amt AS BS1360,fin_stmt_1370_amt AS BS1370,fin_stmt_1400_amt AS BS1400,fin_stmt_1410_amt AS BS1410,fin_stmt_1420_amt AS BS1420,fin_stmt_1430_amt AS BS1430,fin_stmt_1450_amt AS BS1450,fin_stmt_1500_amt AS BS1500,fin_stmt_1510_amt AS BS1510,fin_stmt_1520_amt AS BS1520,NVL(fin_stmt_1530_amt,0) AS BS1530,fin_stmt_1540_amt AS BS1540,fin_stmt_1550_amt AS BS1550,fin_stmt_1600_amt AS BS1600,fin_stmt_1700_amt AS BS1700
                |,NVL(fin_stmt_1100_amt,0) AS BS1100
                |,NVL(fin_stmt_1110_amt,0) AS BS1110
                |,NVL(fin_stmt_1120_amt,0) AS BS1120
                |,NVL(fin_stmt_1130_amt,0) AS BS1130
                |,NVL(fin_stmt_1140_amt,0) AS BS1140
                |,NVL(fin_stmt_1150_amt,0) AS BS1150
                |,NVL(fin_stmt_1160_amt,0) AS BS1160
                |,NVL(fin_stmt_1170_amt,0) AS BS1170
                |,NVL(fin_stmt_1180_amt,0) AS BS1180
                |,NVL(fin_stmt_1190_amt,0) AS BS1190
                |,NVL(fin_stmt_1200_amt,0) AS BS1200
                |,NVL(fin_stmt_1210_amt,0) AS BS1210
                |,NVL(fin_stmt_1220_amt,0) AS BS1220
                |,NVL(fin_stmt_1230_amt,0) AS BS1230
                |,NVL(fin_stmt_1240_amt,0) AS BS1240
                |,NVL(fin_stmt_1250_amt,0) AS BS1250
                |,NVL(fin_stmt_1260_amt,0) AS BS1260
                |,NVL(fin_stmt_1300_amt,0) AS BS1300
                |,NVL(fin_stmt_1310_amt,0) AS BS1310
                |,NVL(fin_stmt_1320_amt,0) AS BS1320
                |,NVL(fin_stmt_1340_amt,0) AS BS1340
                |,NVL(fin_stmt_1350_amt,0) AS BS1350
                |,NVL(fin_stmt_1360_amt,0) AS BS1360
                |,NVL(fin_stmt_1370_amt,0) AS BS1370
                |,NVL(fin_stmt_1400_amt,0) AS BS1400
                |,NVL(fin_stmt_1410_amt,0) AS BS1410
                |,NVL(fin_stmt_1420_amt,0) AS BS1420
                |,NVL(fin_stmt_1430_amt,0) AS BS1430
                |,NVL(fin_stmt_1450_amt,0) AS BS1450
                |,NVL(fin_stmt_1500_amt,0) AS BS1500
                |,NVL(fin_stmt_1510_amt,0) AS BS1510
                |,NVL(fin_stmt_1520_amt,0) AS BS1520
                |,NVL(fin_stmt_1530_amt,0) AS BS1530
                |,NVL(fin_stmt_1540_amt,0) AS BS1540
                |,NVL(fin_stmt_1550_amt,0) AS BS1550
                |,NVL(fin_stmt_1600_amt,0) AS BS1600
                |,NVL(fin_stmt_1700_amt,0) AS BS1700
                |--Флаги проверки на NULL для 15-и обязательных показателей
                |,CASE WHEN fin_stmt_1100_amt is null then FALSE else TRUE END AS BS1100_OK
                |,CASE WHEN fin_stmt_1200_amt is null then FALSE else TRUE END AS BS1200_OK
                |,CASE WHEN fin_stmt_1210_amt is null then FALSE else TRUE END AS BS1210_OK
                |,CASE WHEN fin_stmt_1220_amt is null then FALSE else TRUE END AS BS1220_OK
                |,CASE WHEN fin_stmt_1230_amt is null then FALSE else TRUE END AS BS1230_OK
                |,CASE WHEN fin_stmt_1300_amt is null then FALSE else TRUE END AS BS1300_OK
                |,CASE WHEN fin_stmt_1400_amt is null then FALSE else TRUE END AS BS1400_OK
                |,CASE WHEN fin_stmt_1430_amt is null then FALSE else TRUE END AS BS1430_OK
                |,CASE WHEN fin_stmt_1500_amt is null then FALSE else TRUE END AS BS1500_OK
                |,CASE WHEN fin_stmt_1510_amt is null then FALSE else TRUE END AS BS1510_OK
                |,CASE WHEN fin_stmt_1520_amt is null then FALSE else TRUE END AS BS1520_OK
                |,CASE WHEN fin_stmt_1530_amt is null then FALSE else TRUE END AS BS1530_OK
                |,CASE WHEN fin_stmt_1540_amt is null then FALSE else TRUE END AS BS1540_OK
                |,CASE WHEN fin_stmt_1550_amt is null then FALSE else TRUE END AS BS1550_OK
                |,CASE WHEN fin_stmt_1600_amt is null then FALSE else TRUE END AS BS1600_OK
                |FROM
                |${config.aux}.fok_fin_stmt_rsbu S1
                |CROSS JOIN ${config.aux}.ProxyLGDin_STG_01_00_COMMON T1 INNER JOIN ${config.aux}.ProxyLGDin_STG_02_00_CUST_BASIS T2 on
                |S1.crm_cust_id=T2.CRM_ID --LIMIT 1
                |and
                |--Отсечка отчетности по дате
                |ADD_MONTHS(
                |month_first_day,-ProxyLGDin_CONST_REP_HIST --кол-во месяцев из настроек
                |)<=
                |ADD_MONTHS( -- Добавляем 1 месяц, чтобы получить отчетную дату (конец квартала+1)
                |CAST(
                |SUBSTR(
                |from_unixtime( --Преобразование unix_date в дату
                |unix_timestamp( --Преобразование в unix_date по формату
                |CONCAT(cast(cast(fin_stmt_year as int) as string),'/',cast(cast(fin_stmt_period*3 as int) as string),'/01') --Строка даты , Period*3 - последний месяц отчетного квартала/года, первое число
                |,'yyyy/M/d')
                |),1,10)
                |as date)
                |,1)""".stripMargin)

    logInserted()
  val Precision = "1" //допустимая погрешность логических проверок 1 тыс.руб.

  val  CheckFormulas =
      s"""
    CASE WHEN
    BS1100 IS NOT NULL
    AND BS1110 IS NOT NULL
    AND BS1120 IS NOT NULL
    AND BS1130 IS NOT NULL
    AND BS1140 IS NOT NULL
    AND BS1150 IS NOT NULL
    AND BS1160 IS NOT NULL
    AND BS1170 IS NOT NULL
    AND BS1180 IS NOT NULL
    AND BS1190 IS NOT NULL
    AND BS1200 IS NOT NULL
    AND BS1210 IS NOT NULL
    AND BS1220 IS NOT NULL
    AND BS1230 IS NOT NULL
    AND BS1240 IS NOT NULL
    AND BS1250 IS NOT NULL
    AND BS1260 IS NOT NULL
    AND BS1300 IS NOT NULL
    AND BS1310 IS NOT NULL
    AND BS1320 IS NOT NULL
    AND BS1340 IS NOT NULL
    AND BS1350 IS NOT NULL
    AND BS1360 IS NOT NULL
    AND BS1370 IS NOT NULL
    AND BS1400 IS NOT NULL
    AND BS1410 IS NOT NULL
    AND BS1420 IS NOT NULL
    AND BS1430 IS NOT NULL
    AND BS1450 IS NOT NULL
    AND BS1500 IS NOT NULL
    AND BS1510 IS NOT NULL
    AND BS1520 IS NOT NULL
    AND BS1530 IS NOT NULL
    AND BS1540 IS NOT NULL
    AND BS1550 IS NOT NULL
  THEN 0 ELSE 1 END AS CHECK_SRC_2,
 |CASE WHEN BS1600>0 THEN 0 ELSE 1 END AS CHECK_SRC_4,
 |CASE WHEN ABS(BS1600-BS1700)<=$Precision THEN 0 ELSE 1 END AS CHECK_SRC_5,
 |CASE WHEN ABS((BS1100+BS1200)-BS1600)<=$Precision THEN 0 ELSE 1 END AS CHECK_SRC_6,
 |CASE WHEN ABS((BS1300+BS1400+BS1500)-BS1700)<=$Precision THEN 0 ELSE 1 END AS CHECK_SRC_7,
 |CASE WHEN ABS((BS1110+BS1120+BS1130+BS1140+BS1150+BS1160+BS1170+BS1180+BS1190)-BS1100)<=$Precision THEN 0 ELSE 1 END AS CHECK_SRC_8,
 |CASE WHEN ABS((BS1210+BS1220+BS1230+BS1240+BS1250+BS1260)-BS1200)<=$Precision THEN 0 ELSE 1 END AS CHECK_SRC_9,
 |CASE WHEN ABS((BS1310+BS1320+BS1340+BS1350+BS1360+BS1370)-BS1300)<=$Precision THEN 0 ELSE 1 END AS CHECK_SRC_10,
 |CASE WHEN ABS((BS1410+BS1420+BS1430+BS1450)-BS1400)<=$Precision THEN 0 ELSE 1 END AS CHECK_SRC_11,
 |CASE WHEN ABS((BS1510+BS1520+BS1530+BS1540+BS1550)-BS1500)<=$Precision THEN 0 ELSE 1 END AS CHECK_SRC_12
      """.stripMargin
val CheckFilter =
  """
   CHECK_SRC_4 + CHECK_SRC_5 + CHECK_SRC_6 + CHECK_SRC_7 + CHECK_SRC_2*CHECK_SRC_8 + CHECK_SRC_2*CHECK_SRC_9 + CHECK_SRC_2*CHECK_SRC_10 + CHECK_SRC_2*CHECK_SRC_11 + CHECK_SRC_2*CHECK_SRC_12
  """.stripMargin


    dashboardName = s"${config.aux}.ProxyLGDin_STG_06_00_FOK_CHECK"
    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_06_00_FOK_CHECK AS
                |SELECT
                |U7M_ID,REP_DATE,
                 $CheckFormulas
                | FROM ${config.aux}.ProxyLGDin_STG_05_00_FOK_ALL""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_07_00_FOK_MAXDATE"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_07_00_FOK_MAXDATE AS
                |SELECT U7M_ID,MAX(REP_DATE) AS MAX_REP_DATE FROM ${config.aux}.ProxyLGDin_STG_06_00_FOK_CHECK
                |WHERE ($CheckFilter)=0 GROUP BY U7M_ID""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_08_00_INT_ALL"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_08_00_INT_ALL AS
                |SELECT
                |T2.u7m_id as u7m_id
                |,add_months(CAST(fin_stmt_start_dt as date),12) AS REP_DATE,--В исходной таблице строка, обещали переделать, пока заглушка (20180313), по неизвестной причине дата начала периода, прибавляем 12 мес. чтобы получить отчетную дату
                |fin_stmt_1100_amt AS BS1100,fin_stmt_1110_amt AS BS1110,fin_stmt_1120_amt AS BS1120,fin_stmt_1130_amt AS BS1130,fin_stmt_1140_amt AS BS1140,fin_stmt_1150_amt AS BS1150,fin_stmt_1160_amt AS BS1160,fin_stmt_1170_amt AS BS1170,fin_stmt_1180_amt AS BS1180,fin_stmt_1190_amt AS BS1190,fin_stmt_1200_amt AS BS1200,fin_stmt_1210_amt AS BS1210,fin_stmt_1220_amt AS BS1220,fin_stmt_1230_amt AS BS1230,fin_stmt_1240_amt AS BS1240,fin_stmt_1250_amt AS BS1250,fin_stmt_1260_amt AS BS1260,fin_stmt_1300_amt AS BS1300,fin_stmt_1310_amt AS BS1310,fin_stmt_1320_amt AS BS1320,fin_stmt_1340_amt AS BS1340,fin_stmt_1350_amt AS BS1350,fin_stmt_1360_amt AS BS1360,fin_stmt_1370_amt AS BS1370,fin_stmt_1400_amt AS BS1400,fin_stmt_1410_amt AS BS1410,fin_stmt_1420_amt AS BS1420,fin_stmt_1430_amt AS BS1430,fin_stmt_1450_amt AS BS1450,fin_stmt_1500_amt AS BS1500,
                |fin_stmt_1510_amt AS BS1510,
                |fin_stmt_1520_amt AS BS1520,
                |fin_stmt_1530_amt AS BS1530,
                |fin_stmt_1540_amt AS BS1540,fin_stmt_1550_amt AS BS1550,
                |fin_stmt_1600_amt AS BS1600,
                |fin_stmt_1700_amt AS BS1700
                | FROM ${config.aux}.INT_fin_stmt_rsbu S1 CROSS JOIN ${config.aux}.ProxyLGDin_STG_01_00_COMMON T1 INNER JOIN ${config.aux}.ProxyLGDin_STG_02_00_CUST_BASIS T2 on S1.cust_inn_num=T2.INN
                |and
                |--Отсечка отчетности по дате
                |ADD_MONTHS(
                |month_first_day,-ProxyLGDin_CONST_REP_HIST --кол-во месяцев из настроек
                |)<=
                |add_months(CAST(fin_stmt_start_dt as date),12)""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_09_00_INT_CHECK"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_09_00_INT_CHECK AS
                |SELECT
                |U7M_ID,REP_DATE,
                $CheckFormulas
                | FROM ${config.aux}.ProxyLGDin_STG_08_00_INT_ALL
                |-- limit 10""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_10_00_INT_MAXDATE"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_10_00_INT_MAXDATE AS
                |SELECT u7m_id,MAX(REP_DATE) AS MAX_REP_DATE FROM ${config.aux}.ProxyLGDin_STG_09_00_INT_CHECK
                WHERE ($CheckFilter)=0
                GROUP BY u7m_id""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_11_00_REP_FIN"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_11_00_REP_FIN AS
                |SELECT BAS.U7M_ID,
                |CASE
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
                |FROM ${config.aux}.ProxyLGDin_STG_02_00_CUST_BASIS BAS
                |LEFT JOIN ${config.aux}.ProxyLGDin_STG_07_00_FOK_MAXDATE F ON BAS.U7M_ID=F.U7M_ID
                |LEFT JOIN ${config.aux}.ProxyLGDin_STG_10_00_INT_MAXDATE I ON BAS.U7m_ID=I.U7M_ID""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_12_00_REP_SRC"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_12_00_REP_SRC AS
                |SELECT
                |BF.U7M_ID
                |--,BF.INN
                |,BF.SRC
                |,BF.MAX_REP_DATE
                |,BS1520,BS1230,BS1600,BS1510,BS1200,BS1500,BS1220,BS1210,BS1530,BS1300,BS1400,BS1100,BS1550,BS1430,BS1540
                | FROM ${config.aux}.ProxyLGDin_STG_11_00_REP_FIN BF
                |JOIN ${config.aux}.ProxyLGDin_STG_05_00_FOK_ALL F
                |ON BF.SRC='FOK' AND BF.U7M_ID=F.U7M_ID AND BF.MAX_REP_DATE=F.REP_DATE
                |UNION ALL
                |SELECT
                |BI.U7M_ID
                |--,BI.INN
                |,BI.SRC
                |,BI.MAX_REP_DATE
                |,BS1520,BS1230,BS1600,BS1510,BS1200,BS1500,BS1220,BS1210,BS1530,BS1300,BS1400,BS1100,BS1550,BS1430,BS1540
                | FROM ${config.aux}.ProxyLGDin_STG_11_00_REP_FIN BI
                |JOIN ${config.aux}.ProxyLGDin_STG_08_00_INT_ALL I
                |ON
                |BI.SRC='INT' AND BI.U7M_ID=I.U7M_ID AND BI.MAX_REP_DATE=I.REP_DATE""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_13_00_CALC_SRC"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_13_00_CALC_SRC AS
                |SELECT
                |U7M_ID
                |--,INN AS INN
                |,BS1520 AS acc_pay_y,BS1230 AS acc_rec_y,BS1600 AS asset_y,BS1510 AS borrowed_capital_y,BS1200 AS cur_asset_y,BS1500 AS cur_liab_y,BS1220 AS curr_VAT_y,BS1210 AS curr_inv_y,BS1530 AS def_rev_y,BS1300 AS equity_y,BS1400 AS n_cur_liab_y,BS1100 AS noncur_assets_y,BS1550 AS other_liabilities_y,BS1430 AS prov_contingent_liabilities_y,BS1540 AS res_exp_y
                |FROM
                |${config.aux}.ProxyLGDin_STG_12_00_REP_SRC""".stripMargin)

    logInserted()

    dashboardName = s"${config.aux}.ProxyLGDin_STG_14_00_CALC_CUST"

    spark.sql(s"""CREATE TABLE ${config.aux}.ProxyLGDin_STG_14_00_CALC_CUST AS
                |SELECT
                |T2.U7M_ID AS U7M_ID
                |--,T2.CRM_ID AS CRM_ID
                |, CASE WHEN T2.attrib_03='ОПК' THEN 1 ELSE 0 END AS dummy_opkflag
                |, noncur_assets_y AS noncur_assets_y
                |, CASE WHEN cur_asset_y=0 THEN 0 ELSE
                |CASE WHEN cur_liab_y=0 THEN ProxyLGDin_CONST_INFINITY ELSE cur_asset_y/cur_liab_y
                |END
                |END AS curliqr_2
                |, CASE WHEN (equity_y + def_rev_y + res_exp_y + prov_contingent_liabilities_y) - noncur_assets_y=0 THEN 0 ELSE
                |CASE WHEN (equity_y + def_rev_y + res_exp_y + prov_contingent_liabilities_y)<0 THEN 0 ELSE
                |CASE WHEN (equity_y + def_rev_y + res_exp_y + prov_contingent_liabilities_y)=0 THEN ProxyLGDin_CONST_NULL ELSE
                |   ((equity_y + def_rev_y + res_exp_y + prov_contingent_liabilities_y) - noncur_assets_y) /
                |    (equity_y + def_rev_y + res_exp_y + prov_contingent_liabilities_y)
                |END
                |END
                |END AS cas2eqr_2_qa,
                |CASE WHEN acc_rec_y=0 THEN 0 ELSE
                |CASE WHEN asset_y=0 THEN ProxyLGDin_CONST_NULL ELSE
                |acc_rec_y/asset_y
                |END
                |END AS acrecbc_2_qa,
                |CASE WHEN curr_inv_y + curr_VAT_y=0 THEN 0 ELSE
                |CASE WHEN n_cur_liab_y + borrowed_capital_y + acc_pay_y +def_rev_y + other_liabilities_y + res_exp_y=0 THEN ProxyLGDin_CONST_INFINITY ELSE
                |(curr_inv_y + curr_VAT_y)/(n_cur_liab_y + borrowed_capital_y + acc_pay_y +def_rev_y + other_liabilities_y + res_exp_y)
                |END
                |END AS rexp2dbt_QA,
                |CASE WHEN borrowed_capital_y=0 THEN 0 ELSE
                |CASE WHEN asset_y=0 THEN ProxyLGDin_CONST_NULL ELSE
                |CASE WHEN borrowed_capital_y/asset_y>1 THEN 1 ELSE
                |borrowed_capital_y/asset_y
                |END
                |END
                |END AS stl2as_2_QA,
                |CASE WHEN acc_pay_y=0 THEN 0 ELSE
                |CASE WHEN asset_y=0 THEN ProxyLGDin_CONST_NULL ELSE
                |acc_pay_y/asset_y
                |END
                |END AS cr2as_2_QA,
                |CASE WHEN asset_y=0 THEN 0 ELSE
                |CASE WHEN middle_assets=0 THEN ProxyLGDin_CONST_NULL ELSE
                |asset_y/middle_assets
                |END
                |END AS rg2avsbr_sm2_QA,
                |CASE WHEN middle_age=0 THEN ProxyLGDin_CONST_NULL ELSE
                |(DATEDIFF(month_first_day, RegistrationDate)) / (365.25) / middle_age
                |END AS brwisyng_3_sm
                |FROM
                |${config.aux}.ProxyLGDin_STG_01_00_COMMON T1
                |CROSS JOIN ${config.aux}.ProxyLGDin_STG_02_00_CUST_BASIS T2 --возможно обойтись без базиса т.к. u7m_ID теперь должен быть во всех таблицах, а данные отчетности уже с отсечкой по базису
                |INNER JOIN ${config.aux}.ProxyLGDin_STG_13_00_CALC_SRC T13 on T2.U7M_ID=T13.U7M_ID --INNER JOIN, в расчет не попадают  клиенты без данных отчетности""".stripMargin)

    logInserted()

    dashboardName = s"${config.pa}.ProxyLGDin"

    spark.sql(s"""CREATE TABLE ${config.pa}.ProxyLGDin AS
                |SELECT
                |T4.BO_ID  AS BO_ID
                |,T4.u7m_ID
                |,ProxyLGDin_CONST_ANetCCO_to_EAD  AS ANetCCO_to_EAD
                |,ProxyLGDin_CONST_brtclwner3_ok_1_nd  AS brtclwner3_ok_1_nd
                |,ProxyLGDin_CONST_collosn5  AS collosn5
                |,ProxyLGDin_CONST_cnedvokr_nd  AS cnedvokr_nd
                |,ProxyLGDin_CONST_ufn_flag  AS ufn_flag
                |,ProxyLGDin_CONST_maxcolgivrate_osn_1_nd  AS maxcolgivrate_osn_1_nd
                |,CASE WHEN noncur_assets_y=0 THEN 0 ELSE
                |CASE WHEN exp_SB=0 THEN ProxyLGDin_CONST_INFINITY ELSE
                |noncur_assets_y/T4.exp_SB
                |END
                |END AS das2dbt_3_QA
                |,T2.U7M_ID  AS CLIENT_ID --Так было в первоначальной постановке, возможно требуется переименовать в U7M_ID
                |,dummy_opkflag  AS dummy_opkflag
                |,noncur_assets_y*1000  AS noncur_assets_y
                |,curliqr_2  AS curliqr_2
                |,cas2eqr_2_qa  AS cas2eqr_2_qa
                |,acrecbc_2_qa  AS acrecbc_2_qa
                |,rexp2dbt_QA  AS rexp2dbt_QA
                |,stl2as_2_QA  AS stl2as_2_QA
                |,cr2as_2_QA  AS cr2as_2_QA
                |,rg2avsbr_sm2_QA  AS rg2avsbr_sm2_QA
                |,brwisyng_3_sm  AS brwisyng_3_sm
                |FROM ${config.aux}.ProxyLGDin_STG_02_00_CUST_BASIS T2 cross join ${config.aux}.ProxyLGDin_STG_01_00_COMMON T1
                |INNER JOIN ${config.aux}.ProxyLGDin_STG_04_00_BO_EXP_U T4 ON T2.U7M_ID=T4.U7M_ID
                |INNER JOIN ${config.aux}.ProxyLGDin_STG_14_00_CALC_CUST T14 on T2.U7M_ID=T14.U7M_ID""".stripMargin)

    logInserted()

    dashboardName = tblBoLGD
    spark.table(s"${config.aux}.bo_empty")
      .write
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.auxPath}bo_lgd")
      .format("parquet")
      .saveAsTable(tblBoLGD)

    Seq(
      s"INSERT INTO $tblBoLGD select '$execId' as exec_id,null as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,'ANetCCO_to_EAD' as KEY,null as VALUE_C,cast(cc.ANetCCO_to_EAD as decimal(16,5)) as VALUE_N ,NULL as VALUE_D FROM ${config.pa}.bo_keys B inner join ${config.pa}.ProxyLGDin cc on cc.bo_id = B.bo_id",
      s"INSERT INTO $tblBoLGD select '$execId' as exec_id,null as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,'ufn_flag' as KEY,null as VALUE_C,cast(cc.ufn_flag as decimal(16,5)) as VALUE_N ,NULL as VALUE_D FROM ${config.pa}.bo_keys B inner join ${config.pa}.ProxyLGDin cc on cc.bo_id = B.bo_id",
      s"INSERT INTO $tblBoLGD select '$execId' as exec_id,null as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,'maxcolgivrate_osn_1_nd' as KEY,null as VALUE_C,cast(cc.maxcolgivrate_osn_1_nd as decimal(16,5)) as VALUE_N ,NULL as VALUE_D FROM ${config.pa}.bo_keys B inner join ${config.pa}.ProxyLGDin cc on cc.bo_id = B.bo_id",
      s"INSERT INTO $tblBoLGD select '$execId' as exec_id,null as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,'brtclwner3_ok_1_nd' as KEY,null as VALUE_C,cast(cc.brtclwner3_ok_1_nd as decimal(16,5)) as VALUE_N ,NULL as VALUE_D FROM ${config.pa}.bo_keys B inner join ${config.pa}.ProxyLGDin cc on cc.bo_id = B.bo_id",
      s"INSERT INTO $tblBoLGD select '$execId' as exec_id,null as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,'cnedvokr_nd' as KEY,null as VALUE_C,cast(cc.cnedvokr_nd as decimal(16,5)) as VALUE_N ,NULL as VALUE_D FROM ${config.pa}.bo_keys B inner join ${config.pa}.ProxyLGDin cc on cc.bo_id = B.bo_id",
      s"INSERT INTO $tblBoLGD select '$execId' as exec_id,null as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,'collosn5' as KEY,null as VALUE_C,cast(cc.collosn5 as decimal(16,5)) as VALUE_N ,NULL as VALUE_D FROM ${config.pa}.bo_keys B inner join ${config.pa}.ProxyLGDin cc on cc.bo_id = B.bo_id",
      s"INSERT INTO $tblBoLGD select '$execId' as exec_id,null as okr_id,B.BO_ID as BO_ID,B.u7m_id as u7m_id,b.clust_id as clust_id,'das2dbt_3_QA_OFFLINE' as KEY,null as VALUE_C,cast(cc.das2dbt_3_QA as decimal(16,5)) as VALUE_N ,NULL as VALUE_D FROM ${config.pa}.bo_keys B inner join ${config.pa}.ProxyLGDin cc on cc.bo_id = B.bo_id"
    ).foreach(spark.sql(_))

    logInserted()


    logEnd()
  }
}