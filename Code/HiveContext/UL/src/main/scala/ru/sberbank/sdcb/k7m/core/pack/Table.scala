package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Table(val config: Config) extends EtlLogger with EtlJob with DashBoardNames {

	val dashboardName: String
	val dashboardPath: String
	val dataframe: DataFrame

	val ZeroLayerSchema: String = config.stg
	val SaveSchema: String = config.aux
	val logSchemaName: String = config.aux

	override def processName: String = "UL"

	val spark: SparkSession = SparkSession.builder
		.appName(processName)
		.enableHiveSupport()
		.getOrCreate

	val DATE: String = LoggerUtils.obtainRunDtWithoutTime(spark, logSchemaName)

	val TP_MAX_COMP = 10
	val NRD_INN = "7702165310"
	val NUMBER_APPL = 4
	val FAMILY_SIZE = 8
	val GEN_DIR_COMP_COUNT = 11
	val SBRF_INN = "7707083893"
	val custom_inn = s"${config.aux}.basis_client"

	val CRM_CLEAR_COUNT = 3

	def genDashBoardName(dashboardShortName: String): String = SaveSchema + "." + dashboardShortName

	def genDashBoardPath(dashboardShortName: String): String = config.auxPath + dashboardShortName

	def drop(): Unit = spark.sql(s"drop table if exists $dashboardName")

	def save(): Unit = {
		logStart()
		drop()
		println(s"table $dashboardName in progress")

		dataframe
			.write
			.format("parquet")
			.option("path", dashboardPath)
			.mode("overwrite")
			.saveAsTable(dashboardName)
		logInserted()
		logEnd()

	}

	def save(df: DataFrame, name: String, path: String): Unit = {
		spark.sql(s"drop table if exists $name")
		println(s"table $name in progress")

		df
			.write
			.format("parquet")
			.option("path", path)
			.mode("overwrite")
			.saveAsTable(name)

	}

	def append(): Unit = {
		logStart()
		dataframe
			.write
			.format("parquet")
			.option("path", dashboardPath)
			.mode("append")
			.saveAsTable(dashboardName)
		logInserted()
		logEnd()
	}

	def append(df: DataFrame, name: String, path: String): Unit = {
		df
			.write
			.format("parquet")
			.option("path", path)
			.mode("append")
			.saveAsTable(name)
	}

//	def saveAndLog(): Unit = {
//		logStart()
//		save()
//		logInserted()
//		logEnd()
//	}

}
