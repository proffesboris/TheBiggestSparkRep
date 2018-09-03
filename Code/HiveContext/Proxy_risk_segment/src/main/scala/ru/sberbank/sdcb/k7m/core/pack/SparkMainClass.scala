package ru.sberbank.sdcb.k7m.core.pack
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMainClass extends BaseMainClass {


  /**
    * Метод исполнения построения витрин
    *
    * @param params параметр
    * @param config конфигурация окружения
    * @param execId идентификатор расчета
    */
  override def run(params: Map[String, String], config: Config): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("proxy_risk_segment")
      .getOrCreate()

    val  ProxyRiskSeg =   (new ProxyRiskSegmentClass(spark, config)).DoProxyRiskSegment()
  }

}

