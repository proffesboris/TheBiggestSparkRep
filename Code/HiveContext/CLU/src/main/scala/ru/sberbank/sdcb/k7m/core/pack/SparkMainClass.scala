package ru.sberbank.sdcb.k7m.core.pack
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMainClass extends BaseMainClass {

  val limitKey = "limit"

  val cCodes = s"""'to_close', 'work', 'in_support', 'to_buh_control'"""

  val applicationName = "CLU"

  //---------------------------------------------------------
  override def run(params: Map[String, String], config: Config): Unit = {

    //---------------------------------------------------------
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName(applicationName)
      .getOrCreate()

  val limit: Int = params.getOrElse(limitKey,"0").toInt

    val processLogger = new ProcessLogger(spark, config, applicationName) // processName- наименование процесса,
    processLogger.logStartProcess()

    val CLUGroupDF = new CLUGroupClass(spark, config)
    val ClientCrmClu = new ClientCrmCluClass(spark,config)
    val ClEksClu = new ClientEksCluClass(spark, config)
    val EksCluAddress = new EksCluAddressClass(spark, config)
    val FnsCluRegAddress = new FnsCluRegAddressClass(spark, config)
    val ClientGuarDF = new ClientGuarClass(spark, config)
    val ClientCreditDF = new ClientCreditClass(spark, config)
    val ClientGenAgreemFrameDF = new ClientGenAgreemFrameClass(spark, config)
    val ClientCrmKredDF = new ClientCrmKredClass(spark, config)
    val ClientIntContactsClu = new ClientIntContactsCluClass(spark, config)
    val ClientContractData = new ClientContractDataClass(spark, config)
    val FokAssetsHeaderFinalRSBU = new FokAssetsHeaderFinalRSBUClass(spark, config)
    val FokAssets = new FokAssetsClass(spark, config)
    val FokAssetsRosstat = new FokAssetsRosstatClass(spark, config)
    val CluAssetsRosstat = new CluAssetsRosstatClass(spark, config)
    val CluAssets = new CluAssetsClass(spark, config)
    val Clu = new CluClass(spark, config)


    CLUGroupDF.DoCLUGroup()
    ClientCrmClu.DoClientCrmClu()
    ClEksClu.DoClientEksClu()
    EksCluAddress.DoEksCluAddress()
    FnsCluRegAddress.DoFnsCluRegAddress()
    ClientGuarDF.DoClientGuar()
    ClientCreditDF.DoClientCredit()
    ClientGenAgreemFrameDF.DoClientGenAgreemFrame()
    ClientCrmKredDF.DoClientCrmKred()
    ClientIntContactsClu.DoClientIntContactsClu()
    ClientContractData.DoClientContractData()
    FokAssetsHeaderFinalRSBU.DoFokAssetsHeaderFinalRSBU()
    FokAssets.DoFokAssets()
    FokAssetsRosstat.DoFokAssetsRosstat()
    CluAssetsRosstat.DoCluAssetsRosstat()
    CluAssets.DoCluAssets()
    Clu.DoClu(limit)

    processLogger.logEndProcess()
  }
}

