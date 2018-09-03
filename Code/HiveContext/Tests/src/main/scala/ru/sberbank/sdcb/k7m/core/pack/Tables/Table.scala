package ru.sberbank.sdcb.k7m.core.pack.Tables

import org.apache.spark.sql.SparkSession

abstract class Table(spark: SparkSession) {

	val tableName: String
	val tableSchema: String

	val creationQuery: String

	def dropTable(): Unit = spark.sql(s"drop table if exists $tableSchema.$tableName")

	def createTable(): Unit = spark.sql(creationQuery)

	def dropAndCreate(): Unit = {
		dropTable()
		createTable()
	}

}
