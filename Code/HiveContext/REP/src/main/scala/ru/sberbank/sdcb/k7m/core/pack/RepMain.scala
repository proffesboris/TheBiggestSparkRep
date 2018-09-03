package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object RepMain extends BaseMainClass{

  val dateString: String = s"""2018-02-02"""

  val mainVId: String = s"""91414%"""

  val clPriv: String = s"""CL_PRIV"""


	override def run(params: Map[String, String], config: Config): Unit = {

    val spark = SparkSession
      .builder()
      .appName("REP")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", "10000")
      .config("hive.exec.max.dynamic.partitions.pernode","10000")
      .getOrCreate()

      val processLogger = new ProcessLogger(spark, config, "REP") // processName- наименование процесса,
      processLogger.logStartProcess()

      new repClass(spark,config).DoRep()

      processLogger.logEndProcess()
	}


}
