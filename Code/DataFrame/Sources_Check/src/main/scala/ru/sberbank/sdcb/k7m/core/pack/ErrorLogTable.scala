package ru.sberbank.sdcb.k7m.core.pack

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}

import scala.collection.mutable

class ErrorLogTable(spark: SparkSession, schema: String) {

	private val tableName: String = "sourceerrors"
	private val fullName = schema + "." + tableName

	private val tableCreationQuery =
		s"""
			|create table $fullName(
			|		problemTable String,
			|		problemType String,
			|		SQLstatement String,
			|		StackTrace String
			|)
		""".stripMargin

	private val noTableErrorCode = "NO SUCH TABLE"
	private val sqlCodeError 		 = "NO SUCH FIELD"
	private val permissionError  = "PERMISSION DENIED"
	private val unknownError     = "UNKNOWN ERROR"
	private val typeMismatchError     = "TYPE MISMATCH ERROR"

	def trySelect1Statement(tableName: String): Boolean = {
		try {
			spark.table(tableName).show(1)
      true
		} catch {
			case rex: RuntimeException =>
        logPermissionError(tableName, stackTraceToString(rex))
        false
      case ex: Exception 				 =>
        logUnknownError(tableName, stackTraceToString(ex))
        false
    }
	}

	def tryExplainStatement(tableName: String, sqlStatement: String): Boolean = {
		try {
			spark.sql(sqlStatement).explain
      true
		} catch {
			case aex: AnalysisException =>
        logNoFieldError(tableName, sqlStatement, stackTraceToString(aex))
        false
			case ex:  Exception 				=>
        logUnknownError(tableName, sqlStatement, stackTraceToString(ex))
        false
		}
	}

	def checkFieldTypes(tableName: String, fieldsTypesMap: mutable.Map[String, String]): Boolean = {
		import spark.implicits._
    val describeQuery = s"describe $tableName"
    val desc = spark.sql(describeQuery)
    var result = true
		fieldsTypesMap.foreach(r => {
      val fieldName = r._1
      val typeInS2t = r._2
      val rows = desc.where(lower($"col_name") === fieldName).select($"data_type").collectAsList()
			if (rows.isEmpty) {
        logTypeMismatchError(tableName, describeQuery, s"field($fieldName) not present in table")
        result = false
			} else {
        val typeInSource = rows.get(0).getString(0)
        if (typeInSource != typeInS2t) {
          logTypeMismatchError(tableName, describeQuery, s"field($fieldName) type mismatch: s2t($typeInS2t) != source($typeInSource)")
          result = false
        }
      }
		})
    result
	}

	def stackTraceToString(t: Throwable): String = {
		val sw = new StringWriter
		t.printStackTrace(new PrintWriter(sw))
		sw.toString.replace("\n", "\t")
	}

	def errorsCount(): Long = spark.table(fullName).count

	def logNoTableError(tableName: String): Unit = {
		insertIntoTable(Seq((
			tableName,
			noTableErrorCode,
			s"select * from $tableName limit 1",
			"NULL")))
	}

	def logNoFieldError(tableName: String, sqlStatement: String, trace: String): Unit = {
		insertIntoTable(Seq((
			tableName,
			sqlCodeError,
			sqlStatement,
			trace)))
	}

	def logTypeMismatchError(tableName: String, sqlStatement: String, trace: String): Unit = {
		insertIntoTable(Seq((
			tableName,
			typeMismatchError,
			sqlStatement,
			trace)))
	}

	def logPermissionError(tableName: String, trace: String): Unit = {
		insertIntoTable(Seq((
			tableName,
			permissionError,
			s"select * from $tableName limit 1",
			trace)))
	}

	def logUnknownError(tableName: String, trace: String): Unit = {
		insertIntoTable(Seq((
			tableName,
			unknownError,
			s"select * from $tableName limit 1",
			trace)))
	}

	def logUnknownError(tableName: String, sqlStatement: String, trace: String): Unit = {
		insertIntoTable(Seq((tableName, unknownError, sqlStatement, trace)))
	}

	private def insertIntoTable(data: Seq[(String, String, String, String)]): Unit = {
		spark.createDataFrame(data).toDF("problemTable", "problemType", "SQLstatement", "StackTrace")
			.write.mode(SaveMode.Append).insertInto(fullName)
	}

	def dropNsave(): Unit = {
		dropTable()
		createTable()
	}

	private def createTable(): Unit = spark.sql(tableCreationQuery)
	private def dropTable(): Unit = spark.sql(s"drop table if exists $fullName")
}

object ErrorLogTable {

	def apply(spark: SparkSession, schema: String): ErrorLogTable = new ErrorLogTable(spark, schema)

}
