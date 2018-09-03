package ru.sberbank.sdcb.k7m.core.pack.utils

import org.apache.spark.sql.SparkSession
import ru.sberbank.sdcb.k7m.core.pack.{Config, EtlJob, EtlLogger}

class TestsLogger(val spark: SparkSession,
									val dashboardName: String,
									val config: Config, val level: String) extends EtlLogger with EtlJob {

	override def processName: String = s"CHECK$level"

}