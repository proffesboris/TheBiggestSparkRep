package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql.SparkSession

object SparkMain extends BaseMainClass {

  val STAGE_ID_KEY: String = "stageId"

  val CONST_REVENUE_UP_LIMIT = "1.2"
  val CONST_REVENUE_DOWN_LIMIT = "0.8"
  val CONST_REVENUE_CUT_OFF_LOW = "0.1"
  val CONST_REVENUE_CUT_OFF_HIGH = "3"
  val revenueDownLimit = "1000000"
  val SBRF_INN = "7707083893"

  val CONST_COND_DOWN = "0.03"
  val CONST_TOP_HEADONTR_COUNT = "35"
  val CONST_TOP_PAYOFFLOANS_COUNT = "15"
  val CONST_DOWN_LOANS_SHARE = "0.02"

  val innSber = SBRF_INN

  val payOffLoans = "'гашение займа'"

  val receiveLoans = "'выдача займа'"

  val consultIn1 = "'%консульт%'"
  val consultIn2 = "'%информац%'"
  val consultIn3 = "'%услуг%'"

  val excludeStorage = "'dsdfsdfsfsf', '-', ''"

  val excludeStoragedsf = "'dsdfsdfsfsf', ''"

  val excludeInn = "'', '0', '000000000', '0000000000', '000000000000'"

  val includeName = "'Кредиты/гарантии/аккредитивы'"

  val excludeTypeCD = "'Филиал','Территория','Холдинг'"

  val includeStg = "'10.Закрыта/Заключена'"

  val excludeRevnStatCD = "'Отклонен'"

  val includeCollXSortCD1 = "'%ПОР%'"

  val includeCollXSortCD2 = "'%Ю%'"

  val dateStringIntegrum = "2999-12-31"

  val closedStatus = "'Закрыта'"

  val partyTypeOrg = "'Organization'"

  val applicationName = "SBL"
  override def run(params: Map[String, String], config: Config): Unit = {


    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(applicationName)
      .getOrCreate()

    val date = LocalDate.parse(params.getOrElse("date", LoggerUtils.obtainRunDtWithoutTime(spark,config.aux)))
    val paramDate = params.getOrElse("date", LoggerUtils.obtainRunDtWithoutTime(spark,config.aux))
    val paramDateIntegrum = params.getOrElse("dateInt", dateStringIntegrum)

    val processLogger = new ProcessLogger(spark, config, applicationName)

    processLogger.logStartProcess()

    val SBLKrasDF = (new SBLTrBasisKrasClass(spark, config)).DoSBLTrBasisKras()

    new Target_Revenue_Rosstat(spark, config, date).run(revenueDownLimit)
    new Target_revenue_L12M(spark, config, date).run()
    new Target_revenue_LY(spark, config, date).run()
    new Revenue_Target(spark, config, date).run(paramDate)

    new Dividends_IN(spark, config, date).run()
    new Dividends_OUT(spark, config, date).run()
    new Coworking(spark, config, date).run()
    new Armlirt_Ratings(spark, config, date).run()
    new Crit_Consgr(spark, config, date).run()

    val CritHeadonTrDF = new CritHeadonTr(spark, config).run(date)

    val critPayoffloansDF = new CritPayoffloans(spark, config).run(date)

    val IntIpEgripDF = (new  IntIpEgripClass(spark, config)).DoIntIpEgrip(paramDate)

    val SBL3DF = (new SBL3Class(spark, config)).DoSBL3CritnpLoans(paramDate)

    val SBL4DF = (new SBL4Class(spark, config)).DoSBL4CritHeadOnLoan(paramDate)

    val SBL5DF = (new SBL5Class(spark, config)).DoSBL5CritDocapit(paramDate)

    val SBL8DF = (new SBL8Class(spark, config)).DoSBL8CritPayoffloansOut(paramDate)

    val SBL9DF = (new SBL9Class(spark, config)).DoSBL9CritRecLoans(paramDate)

    val SBL10DF = (new SBL10Class(spark, config)).DoSBL10CritSendLoans(paramDate)

    val SBL11DF = (new SBL11Class(spark, config)).DoSBL11CritConsIn(paramDate)

    val SBL13DF = (new SBL13Class(spark, config)).DoSBL13CritAdrEgrul(paramDate, paramDateIntegrum)

    val SBL14DF = (new SBL14Class(spark, config)).DoSBL14CritAdrRosstat(paramDate, paramDateIntegrum)

    val SBL15DF = (new SBL15Class(spark, config)).DoSBL15CritZalogi(paramDate)

    val SBL16DF = (new SBL16Class(spark, config)).DoSBL16CritBorrowers(paramDate)

    val SBL17DF = (new SBL17Class(spark, config)).DoSBL17CritFounders50(paramDate, paramDateIntegrum)

    val SBL18DF = (new SBL18Class(spark, config)).DoSBL18CritCRMLK()

    val SBLAllDF = (new SBLAllClass(spark, config)).DoSBLAll()

    processLogger.logEndProcess()
  }

}
