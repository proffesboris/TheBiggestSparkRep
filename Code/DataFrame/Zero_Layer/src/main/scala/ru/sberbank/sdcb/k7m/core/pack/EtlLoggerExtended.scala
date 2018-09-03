package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

class EtlLoggerExtended(val spark: SparkSession, val dashboardName: String, val config: Config) extends EtlLogger with EtlJob {

	private val ERROR_CODE = 1L

	/** Error Messages to Run Table */
	private val ERROR_NO_TABLE_IN_S2T_RUN = s"ERROR: No table $dashboardName in S2T"
	private val ERROR_NO_TABLE_IN_HIVE_RUN = s"ERROR: No table $dashboardName in Hive databases"
	private val ERROR_EMPTY_TABLE_RUN = s"ERROR: table $dashboardName is empty"

	override def processName: String = "ZERO LAYER"

  def errorNoTableInS2T(errorCode: Long = ERROR_CODE): Unit = {
		insertIntoCustomLogTable("", ERROR_NO_TABLE_IN_S2T_RUN, CustomLogStatus.ERROR)
	}

	def errorNoTableInHive(errorCode: Long = ERROR_CODE): Unit = {
		insertIntoCustomLogTable("", ERROR_NO_TABLE_IN_HIVE_RUN, CustomLogStatus.ERROR)
	}

	def errorSourceTableEmpty(errorCode: Long = ERROR_CODE): Unit = {
		insertIntoCustomLogTable("", ERROR_EMPTY_TABLE_RUN, CustomLogStatus.ERROR)
	}

}