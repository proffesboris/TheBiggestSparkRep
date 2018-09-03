package ru.sberbank.sdcb.k7m.core.pack.Tables

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lower, trim}
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main}
import ru.sberbank.sdcb.k7m.core.pack.utils.Check

import scala.sys.process._

class OfflineChecks(spark: SparkSession, config: Config, val `type`: String) extends Table(spark: SparkSession){

	import spark.implicits._

	val tableName = "ETL_OFFLINE_CHECK"
	val tableSchema: String = config.aux

	private val fileName = "Check"
	val fullName: String = tableSchema + "." + tableName

	val creationQuery: String =
		s"""
			 |create table if not exists $fullName(
			 |CHECK_NUM string, --Порядковый номер проверки
			 |CHECK_NAME string, --Наименование проверки
			 |CHECK_BASE_CD string, --База проверки код
			 |CHECK_CODE string, --Бизнес код проверки
			 |CHECK_DEPENDENCE_CD string, --Зависимость от других проверок
			 |CHECK_BASE_DESC string, --База проверки описание
			 |CHECK_ANALYST_NAME string, --Ответственный аналитик
			 |CHECK_LVL string, --Уровень проверки (1,2,3)
			 |CHECK_TGT_TABLE string, --Таблица результат проверки
			 |CHECK_SQL_TEXT string --Алгоритм Проверки
			 |)
			 |row format delimited fields terminated by ";"
			 |stored as textfile
			 |tblproperties("skip.header.line.count"="1")
			 """.stripMargin

	def createTestsTable(filePath: String): Unit = {
		copyFile(filePath)
		dropTable()
		createTable()
		replaceEmptyWithNulls()
		loadDataIntoS2Ttable(filePath)
	}

	def genChecksListsForLvl(lvl: Int): List[Check] = spark.table(fullName)
		.where($"check_lvl" === lvl &&
				   $"check_sql_text".isNotNull &&
				   $"check_tgt_table".isNotNull &&
				   $"check_dependence_cd".isNull)
		.select(trim($"check_sql_text").as("check_sql_text"),
			      trim($"check_tgt_table").as("check_tgt_table"),
			      trim($"check_code").as("check_code"))
		.collect
		.map(r => Check(r.getAs[String]("check_code"),
										replaceAllSchemas(r.getAs[String]("check_sql_text")),
										r.getAs[String]("check_tgt_table")))
		.toList

	def genFinalChecks(checkName: String): List[Check] = spark.table(fullName)
		.where(lower($"CHECK_DEPENDENCE_CD") === checkName)
		.select(trim($"check_sql_text").as("check_sql_text"),
			      trim($"check_tgt_table").as("check_tgt_table"),
			      trim($"check_code").as("check_code"))
		.collect
		.map(r => Check(r.getAs[String]("check_code"),
										replaceAllSchemas(r.getAs[String]("check_sql_text")),
										r.getAs[String]("check_tgt_table")))
		.toList


	import util.matching.Regex
	def replaceVars(r: Regex)(getVar: String => String) = {
		def replacement(m: Regex.Match) = {
			import java.util.regex.Matcher
			require(m.groupCount == 1)
			Matcher.quoteReplacement( getVar(m group 1) )
		}
		(s: String) => r.replaceAllIn(s, replacement _)
	}

	val r = """\{\{([^{}]+)\}\}""".r
	val m = Map("schema_k7m_pa" -> config.pa,
              "schema_k7m_aux" -> config.aux,
              "schema_k7m_stg" -> config.stg,
              "schema_k7m_bkp" -> config.bkp)
	val replaceAllSchemas = replaceVars(r)( m.withDefaultValue(config.aux) )

	private def copyFile(filePath: String): Unit = {
		s"""hdfs dfs -cp -f $filePath$fileName.csv $filePath$fileName-StageLoad.csv""".!
	}

	private def replaceEmptyWithNulls(): Unit = {
		spark.sql(s"""alter table $fullName set serdeproperties ('serialization.null.format' = '')""")
	}

	private def loadDataIntoS2Ttable(filePath: String): Unit = {
		spark.sql(s"""load data inpath '$filePath$fileName-StageLoad.csv' overwrite into table $fullName""")
	}

}

object OfflineChecks {

	def apply(spark: SparkSession, config: Config, `type`: String): OfflineChecks = new OfflineChecks(spark, config, `type`)

}