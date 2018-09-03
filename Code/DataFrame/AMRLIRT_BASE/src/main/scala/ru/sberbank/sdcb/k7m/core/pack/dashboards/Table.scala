package ru.sberbank.sdcb.k7m.core.pack.dashboards

import org.apache.spark.sql.functions.{broadcast, col, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.{AmrlirtMain, Config, EtlJob, EtlLogger}
import org.apache.spark.sql.types.DecimalType

abstract class Table(val config: Config) extends EtlLogger with EtlJob {

	val dashboardName: String
	val dataFrame: DataFrame
	val dashboardPath: String

	val spark: SparkSession = SparkSession.builder
		.appName("AMRLIRT_BASE")
		.enableHiveSupport()
		.getOrCreate

	import spark.implicits._

	override def processName: String = "AMRLIRT_BASE"


	val source: String = config.stg
	val pa_schema: String = config.pa
	val aux_schema: String = config.aux

	val mt_rows_columns1: DataFrame = spark.table(s"$source.amr_mt_rows_columns").alias("mt_rows_columns1")
	val mt_rows_columns2: DataFrame = spark.table(s"$source.amr_mt_rows_columns").alias("mt_rows_columns2")

	val model_tables: DataFrame = spark.table(s"$source.amr_model_tables")
	val models_tables_values: DataFrame = spark.table(s"$source.amr_models_tables_values")
	val data_types: DataFrame = spark.table(s"$source.amr_data_types")


	val BaseJoinsDataframe: DataFrame = model_tables
		.join(broadcast(mt_rows_columns1), model_tables("model_table_id") === col("mt_rows_columns1.model_table_id"))
		.join(broadcast(mt_rows_columns2), model_tables("model_table_id") === col("mt_rows_columns2.model_table_id"))
		.join(broadcast(models_tables_values), col("mt_rows_columns1.mt_row_column_id") === models_tables_values("mtv_id_col") &&
			col("mt_rows_columns2.mt_row_column_id") === models_tables_values("mtv_id_row"))
		.join(data_types, data_types("data_type_id") === models_tables_values("data_type_id"))
		.where(
			model_tables("is_active") === "Y" &&
			model_tables("model_id").isNull
		).cache()


	def castFieldsToDecimal(df: DataFrame, columns: Seq[String]): DataFrame = {
		var dataFrame = df
		columns.foreach(column =>
			dataFrame = dataFrame.withColumn(column, regexp_replace($"$column", ",", ".").cast(DecimalType(38,18)))
		)
		dataFrame
	}

	private def save(): Unit = {
		drop()
		dataFrame.write.format("parquet").mode("overwrite").option("path", dashboardPath).saveAsTable(s"$dashboardName")
	}

	private def drop(): Unit = {
		spark.sql(s"drop table if exists $dashboardName")

	}

	def saveAndLog(): Unit = {
		logStart()
		save()
		logInserted()
		logEnd()
	}

}
