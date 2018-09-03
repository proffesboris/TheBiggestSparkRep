package ru.sberbank.sdcb.k7m.core.pack
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkMainClass extends BaseMainClass {

  //---------------------------------------------------------
  override def run(params: Map[String, String], config: Config): Unit = {
    //---------------------------------------------------------

    val DevSchema = config.aux
    val MartSchema = config.pa

    //---------------------------------------------------------
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("EKS_EIOGEN")
      .getOrCreate()

    LoggerUtils.createTables(spark, DevSchema) //создание таблиц через if not exists

    val processLogger = new ProcessLogger(spark, config, "EKS_EIOGEN") // processName- наименование процесса,
    processLogger.logStartProcess()

    new EksEiogenClass(spark, config).DoEksEiogen()

    processLogger.logEndProcess()
  }
}


