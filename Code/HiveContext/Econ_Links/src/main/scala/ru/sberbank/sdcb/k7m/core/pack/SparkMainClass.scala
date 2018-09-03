package ru.sberbank.sdcb.k7m.core.pack

/**
  * Created by sbt-medvedev-ba on 08.02.2018.
  */

import org.apache.spark.sql.SparkSession



object SparkMainClass extends BaseMainClass {

  //---------------------------------------------------------

  //val SourceSchemaBusinessServ = "team_business_services"

  val fsColumnNm = "'На дату отч. периода'"

  val fsLineNum1600 = "'1600'"

  val fsLineNumProfit = "'2110'"

  val fsLineNumTotal = "'2220','2350','2210','2120'"

  val formId = "'SB0121'"

  val excludeSBRFInn = "'5249072845'"

  val excludeDivid = "'1-1CGATR'"

  val fieldName = "'PU1.2210.3','PU1.2350.3','PU1.2220.3','PU1.2110.3','PU1.2120.3','MEASURE'"

  val listCOperIssue = s"""--выдачи
      2052198 , 2052228"""

  val listCOperPayment = s"""--Гашения
      2052201,
    2052204,
    2052384,
    2052417,
    2052438,
    2052468,
    64828926174,
    114471178372,
    114471178660,
    114471178716,
    266761003265,
    288061977365,
    288061977381,
    910050126028,
    921448827992,
    921448828072,
    921448828084,
    921448828088,
    921448828092,
    921448828104,
    2922900801244,
    2922900801250,
    2922900801283,
    2922900801290,
    4505159680210,
    5104943125537,
    5104945093024"""

  val cNameAccount = s"""2049610,
    2049634,
    213649287,
    1189819940,
    5674430852596,
    114471177439,
    114471175621,
    114471175623,
    1189819943,
    1189819973,
    2049586,
    2049588"""

  val cCode = "'MARKET_VB_ACC'"

  val excludeClassId = "CL_PRIV"

  val excludeOperType = "ISSUE"

  val zalogAcc = "91414%"

  val excludeProvKtInn = "'7707083893'"

  val excludeProvDtInn = "'7707083893'"

  val NumDtLow = "'44000'"

  val NumDtHigh = "'45999'"

  val includeYpred = "'выдача займа','беспроцентный займ'"

  val includeYpred5222 = "'оплата по договору','эквайринг', 'инкассация','лизинг','аренда'"

  val qnt5211 = "0.1"

  val qnt5212_5214 = "0.5"

  val qnt5213 = "0.25"

  val qnt5222 = "0.25"

  val amtDownLimit5222 = "1000000"

  val lowLimitRate5222 = "0.8"
  val highLimitRate5222 = "1.2"
  val lowLimit2Rate5222 = "0.1"
  val highLimit2Rate5222 = "3"
  val microLimitRate5222 = "0.05"

  val amtFrom5222 = "0.1"
  val amtTo5222 = "3"

  val applicationName = "EconomicLinks"

  override def run(params: Map[String, String], config: Config): Unit = {
    val DevSchemaCynomys = config.aux

    val Stg0Schema = config.stg

    val DevSchema = config.aux

    val MartSchema = config.pa

    //---------------------------------------------------------


    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(applicationName)
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", "10000")
      .config("hive.exec.max.dynamic.partitions.pernode","10000")
      .getOrCreate()

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

    val paramDate = params.getOrElse("date", LoggerUtils.obtainRunDtWithoutTime(spark,config.aux))

    val paramBeginDate = params.getOrElse("dateBegin",  "2017-01-01")

    val processLogger = new ProcessLogger(spark, config, applicationName) // processName- наименование процесса,
    processLogger.logStartProcess()

    val intOrgDF = new IntegrumOrgClass(spark, config)
    val int1600DF = new Integrum1600Class(spark, config)
    val intOFRDF = new IntegrumOFRClass(spark, config)
    val fokHeadDF = new FOKHeaderClass(spark, config)
    val fokHeadSelDF = new FOKHeaderSelectedClass(spark, config)
    val fokRepDF = new FOKReportsClass(spark, config)
    val fokOFRDF = new FOKOFRClass(spark, config)
    val forInt1600 = new Rep1600FinalClass(spark, config)
    val EKSAllCredDF = new AllCredClass(spark, config)
    val EKSFactOperIssuesDF = new EKSFactOperIssuesClass(spark, config)
    val EKSCurCoursesDF = new EKSCurCoursesClass(spark, config)
    val AccRestDF =   new AccRestClass(spark, config)
    val CredToAccDF =   new CredToAccClass(spark, config)
    val CredBalDF =   new CredBalClass(spark, config)
    val CredBalTotalDF =   new CredBalTotalClass(spark, config)
    val ZalogSwitchDF =   new ZalogSwitchClass(spark, config)
    val ZalogSumDF =   new ZalogSumClass(spark, config)
    val ZalogToCredDF =    new ZalogToCredClass(spark, config)
    val ZalogExpDispDF =   new ZalogExplicitDispClass(spark, config)
    val ZalogUndispDF =   new ZalogUndispClass(spark, config)
    val ZalogImplDispDF =   new ZalogImplicitDispClass(spark, config)
    val zalogDispDF = new ZalogDispensedClass(spark, config)
    val fokOFRPrepDF = new FOKOFRPreparedClass(spark, config)
    val OFRFinalDF = new OFRFinalClass(spark, config)
    val EL5211DF = new EL5211Class(spark, config)
    val EL5212PaymentsDF = new EL5212PaymentsClass(spark, config)
    val EL5212aLinkDF = new EL5212aLinkClass(spark, config)
    val ELMainDocumDF = new ELZMainDocumClass(spark, config)
    val EL5212aDetailDF = new EL5212aDetailClass(spark, config)
    val EL5212aPrepDF = new EL5212aPrepClass(spark, config)
    val EL52125214DF = new EL52125214Class(spark, config)
    val EL5213IssuesDF = new EL5213IssuesClass(spark, config)
    val EL5213PrepDF = new EL5213PrepClass(spark, config)
    val EL5213DF = new EL5213Class(spark, config)
    val EL5222NormYULKDF = new EL5222NormYULKClass(spark, config)
    val EL5222NormYULDDF = new EL5222NormYULDClass(spark, config)
    val EL522212MDF = new EL522212MClass(spark, config)
    val EL5222PrepDF = new EL5222PrepClass(spark, config)
    val EL5222DF = new EL5222Class(spark, config)
    val ELAllDF = new ELAllClass(spark, config)

    intOrgDF.DoIntOrg(paramDate)
    int1600DF.DoInt1600()
    intOFRDF.DoIntOFR()
    fokHeadDF.DoFOKHeader()
    fokHeadSelDF.DoFOKHeaderSelected()
    fokRepDF.DoFOKReports()
    fokOFRDF.DoFOKOFR()
    forInt1600.DoRep1600()
    EKSAllCredDF.DoEKSAllCred()
    EKSFactOperIssuesDF.DoEKSFactOperIssues()
    EKSCurCoursesDF.DoEKSCurCourses()
    AccRestDF.DoAccRest(paramDate, paramBeginDate)
    CredToAccDF.DoCredToAcc()
    CredBalDF.DoCredBal()
    CredBalTotalDF.DoCredBal()
    ZalogSwitchDF.DoZalogSwitch(paramDate)
    ZalogSumDF.DoZalogSum(paramDate)
    ZalogToCredDF.DoZalogToCred()
    ZalogExpDispDF.DoZalogExpDisp()
    ZalogUndispDF.DoZalogUndisp()
    ZalogImplDispDF.DoZalogImplDisp()
    zalogDispDF.DoZalogDisp()
    fokOFRPrepDF.DofokOFRPrep(paramDate)
    OFRFinalDF.DoOFRFinal(paramDate)
    EL5211DF.DoEL5211(paramDate)
    EL5212PaymentsDF.DoEL5212Payments(paramDate)
    EL5212aLinkDF.DoEL5212aLink()
    ELMainDocumDF.DoELZMainDocum(paramDate)
    EL5212aDetailDF.DoEL5212aDetail(paramDate)
    EL5212aPrepDF.DoEL5212aPrep(paramDate)
    EL52125214DF.DoEL52125214(paramDate)
    EL5213IssuesDF.DoEL5213Issues(paramDate)
    EL5213PrepDF.DoEL5213Prep()
    EL5213DF.DoEL5213(paramDate)
    EL5222NormYULKDF.DoEL5222NormYULK()
    EL5222NormYULDDF.DoEL5222NormYULD()
    EL522212MDF.DoEL522212M(paramDate)
    EL5222PrepDF.DoEL5222Prep(paramDate)
    EL5222DF.DoEL5222(paramDate)
    ELAllDF.DoELAll()

    processLogger.logEndProcess()
  }
}
