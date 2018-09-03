package ru.sberbank.sdcb.k7m.core.pack

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

object SparkMain extends BaseMainClass {

  override def run(params: Map[String, String], config: Config): Unit = {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("VDOKROut")
      .getOrCreate()

    val now = Calendar.getInstance().getTime
    //---------------------------------------------------------
    val date = new SimpleDateFormat("yyMMddHHmm").format(now)

    val processLogger = new ProcessLogger(spark, config, s"VDOKROut")
    processLogger.logStartProcess()

    new CL(spark, config, date).run()
    new BO(spark, config, date).run()

    processLogger.logEndProcess()
  }

}
