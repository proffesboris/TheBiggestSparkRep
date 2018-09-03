package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object DictLoadMain extends BaseMainClass {

  val processName = "DictionaryLoad"

  override def run(params: Map[String, String], config: Config): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Dictionary Load")
      .enableHiveSupport()
      .getOrCreate

    val filePath = params("filePath")

    val processLogger = new ProcessLogger(spark, config, processName)
    processLogger.logStartProcess()
    val loader = new CsvLoader(spark, filePath, config, processName)
    loader.load(new BoConfigClustersSrc(config))
    loader.load(new BoConfigCubesSrc(config))
    loader.load(new BoConfigFeaturesSrc(config))
    loader.load(new Dict7mKey(config))
    loader.load(new DictBoPdByScale(config))
    loader.load(new DictLkThresholds(config))
    loader.load(new DictMandatoryKeysOut(config))
    loader.load(new RdmCollaborationListK7MMast(config))
    loader.load(new RdmCountryComplMast(config))
    loader.load(new RdmCountryMainMast(config))
    loader.load(new RdmDealTargetMast(config))
    loader.load(new RdmIntCredHistMast(config))
    loader.load(new RdmLimitGuaranteeMast(config))
    loader.load(new RdmLinkCriteriaMast(config))
    loader.load(new RdmMajorGszMast(config))
    loader.load(new RdmMmzPtyRoleMast(config))
    loader.load(new RdmMmzPtyTypeMast(config))
    loader.load(new RdmProblemStatusMast(config))
    loader.load(new RdmProdStateMast(config))
    loader.load(new RdmRiskProductMast(config))
    loader.load(new RdmRiskSegmentMast(config))
    loader.load(new RdmSetProvTypeMast(config))
    loader.load(new RdmSetRegimesMast(config))
    loader.load(new RdmSetVidCredMast(config))
    loader.load(new Rdoc(config))
    loader.load(new RdmIntOrgMast(config))
    processLogger.logEndProcess()
  }


}
