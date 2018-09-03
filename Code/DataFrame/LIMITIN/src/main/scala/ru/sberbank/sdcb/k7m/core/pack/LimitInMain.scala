package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate
import org.apache.spark.sql.SparkSession

object LimitInMain extends BaseMainClass {

	override def run(params: Map[String, String], config: Config): Unit = {

		val applicationName = "LIMITIN"

		val spark = SparkSession
			.builder()
			.enableHiveSupport()
			.appName(applicationName)
			.getOrCreate()

		val date = LocalDate.parse(params.getOrElse("date", LoggerUtils.obtainRunDtWithoutTime(spark,config.aux)))

		val CalcReq = new AmrCalcReq(config, date); 	CalcReq.saveAndLog()
		val LimitIn = new AmrLimitIn(config, date); 	LimitIn.saveAndLog()

	CalcReq.drop()

	}
}
