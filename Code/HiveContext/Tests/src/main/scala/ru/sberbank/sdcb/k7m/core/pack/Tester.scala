package ru.sberbank.sdcb.k7m.core.pack

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit
import ru.sberbank.sdcb.k7m.core.pack.Tables.{ClfCheck, CluCheck, Error, ScoreCheck}
import ru.sberbank.sdcb.k7m.core.pack.utils.{Check, TestsLogger}

class Tester(spark: SparkSession, config: Config) {

	def tryTestStatement(check: Check, level: String, execId: String): Unit = {

		val Logger = new TestsLogger(spark,check.code , config, level)

		Logger.logStart()

		try {

			spark
				.sql(check.sqlStatement)
				.withColumn("exec_id", lit(execId))
				.write
				.mode(SaveMode.Append)
				.format("parquet")
				.insertInto(getTable(check.tgt))

			Logger.logEnd(step = s"CHECK ${check.code}", status = RunStatus.SUCCESS)

		} catch {

			case ex: Exception => Error(spark, config).insertInto(check.tgt, check.code, check.sqlStatement, stackTraceToString(ex))

			Logger.logEnd(step = s"CHECK + ${check.code}", status = RunStatus.FAILED)
      throw ex
		}
	}

	private def getTable(check_tgt_table: String): String = {
		check_tgt_table match {
			case "CLU_CHECK" => CluCheck(spark, config).fullName
			case "CLF_CHECK" => ClfCheck(spark, config).fullName
			case "SCORE_CHECK" => ScoreCheck(spark, config).fullName
			case _ => s"no valid output table for $check_tgt_table"
		}
	}

	private def stackTraceToString(t: Throwable): String = {
		val sw = new StringWriter
		t.printStackTrace(new PrintWriter(sw))
		sw.toString.replace("\n", "\t")
	}

}

object Tester {

	def apply(spark: SparkSession, config: Config): Tester = new Tester(spark, config)

}