package ru.sberbank.sdcb.k7m.core.pack.Tables

import org.apache.spark.sql.SparkSession
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main}

class ScoreCheck(spark: SparkSession, config: Config) extends Table(spark: SparkSession) {

	override val tableName: String = "score_check"
	override val tableSchema: String = config.pa

	val fullName: String = tableSchema + "." + tableName

	override val creationQuery: String =
		s"""
			 |create table if not exists $fullName(
			 |	u7m_id string,
			 |	check_name string,
			 |	check_lvl tinyint,
			 |	check_flg tinyint,
			 |	exec_id string
			 |) stored as parquet
		 """.stripMargin

}

object ScoreCheck {
	def apply(spark: SparkSession, config: Config): ScoreCheck = new ScoreCheck(spark, config)
}
