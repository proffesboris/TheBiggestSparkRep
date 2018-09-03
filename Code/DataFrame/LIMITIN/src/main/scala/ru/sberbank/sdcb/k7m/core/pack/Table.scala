package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.functions.{broadcast, col, regexp_replace}
import org.apache.spark.sql.types.{DecimalType, DoubleType}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class Table(config: Config) extends EtlLogger with EtlJob {

	val dashboardName: String
	val dataFrame: DataFrame
	val dashboardPath: String

	override def processName: String = "LIMITIN"

	val spark: SparkSession = SparkSession.builder
		.appName(processName)
		.enableHiveSupport()
		.getOrCreate

	import spark.implicits._

	val source: String = config.stg
	val pa_schema: String = config.pa
	val aux_schema: String = config.aux

	val clu: DataFrame = spark.table(s"$pa_schema.clu")
	val fok_fin_stmt_debt: DataFrame = spark.table(s"$aux_schema.fok_fin_stmt_debt")
	val fok_fin_stmt_rsbu: DataFrame = spark.table(s"$aux_schema.fok_fin_stmt_rsbu")
	val fok_fin_stmt_debt_sbrf: DataFrame = spark.table(s"$aux_schema.fok_fin_stmt_debt_sbrf")

	def castFieldsToDecimal(df: DataFrame, columns: Seq[String]): DataFrame = {
		var dataFrame = df
		columns.foreach(column =>
			dataFrame = dataFrame.withColumn(column, regexp_replace($"$column", ",", ".").cast(DecimalType(16,4)))
		)
		dataFrame
	}

	def castFieldsToDouble(df: DataFrame, columns: Seq[String]): DataFrame = {
		var dataFrame = df
		columns.foreach(column =>
			dataFrame = dataFrame.withColumn(column, regexp_replace($"$column", ",", ".").cast(DoubleType))
		)
		dataFrame
	}


	def save(): Unit = {
		drop()
		dataFrame.write.format("parquet").mode("overwrite").option("path", dashboardPath).saveAsTable(s"$dashboardName")
	}

	def drop(): Unit = spark.sql(s"drop table if exists $dashboardName")

	def saveAndLog(): Unit = {
		logStart()
		save()
		logInserted()
		logEnd()
	}

}
