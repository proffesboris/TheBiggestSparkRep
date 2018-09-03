package ru.sberbank.sdcb.k7m.core.pack.Tables

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main}

class Error(spark: SparkSession, config: Config) extends Table(spark: SparkSession) {

	import spark.implicits._

	override val tableName: String = "ETL_OFFLINE_CHECK_ERROR"
	override val tableSchema: String = config.aux

	val fullName: String = tableSchema + "." + tableName

	override val creationQuery: String =
		s"""
			 |create table if not exists $fullName(
			 |		date Timestamp,
			 |  	check_code String,
			 |		check_tgt_table String,
			 |  	check_sql_text String,
			 |    error String
			 |) stored as parquet
		 """.stripMargin

	private val current_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(System.currentTimeMillis()))

	def insertInto(check_tgt_table: String, check_code: String,fm_algorithm: String, error: String ): Unit = {
		Seq((current_date, check_code, check_tgt_table, fm_algorithm, error))
			.toDF("date", "check_code", "check_tgt_table", "check_sql_text", "error")
			.write
			.insertInto(fullName)
	}
}

object Error{
	def apply(spark: SparkSession, config: Config): Error = new Error(spark, config)
}
