package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object RefreshMain extends BaseMainClass {

  override def run(params: Map[String, String], config: Config): Unit = {
    val model = params.getOrElse("model", "")
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("Refresh")
      .getOrCreate()

    model match {
      case "integrum" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "pdingmstandalone")
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfactorsingmstandalone")
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfactorsingmrsrvstandalone")
      }
      case "fok" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfokstandalone")
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfactorsfokstandalone")
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfactorsfokrsrvstandalone")
      }
      case "pravo" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "pdprvstandalone")
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfactorsprvstandalone")
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfactorsprvrsrvstandalone")
      }
      case "rsegm" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "proxyrisksegmentstandalone")
        PimUtils.addPath(spark, config.pa, config.paPath, "proxyfactorsrisksegmentstandalone")
      }
      case "mtx" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "pdstandalone")
      }
      case "limitout" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "limitout")
      }
      case "tr" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "pdeksstandalone")
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfactorseksstandalone")
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfactorseksrsrvstandalone")
      }
      case "gsl" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "pdout")
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfactorsout")
        PimUtils.addPath(spark, config.pa, config.paPath, "pdfactorsrsrvout")
      }
      case "transact" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "basisclasseks")
      }
      case "transact2" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "basisclasseks2")
      }
      case "lgd" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "lgdout")
      }
      case "bo" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "bo_gen")
      }
      case "bbo" => {
        PimUtils.addPath(spark, config.pa, config.paPath, "bbo")
      }
      case _ => throw new IllegalStateException(s"Неправильно передан параметр model=$model")
    }

  }

}
