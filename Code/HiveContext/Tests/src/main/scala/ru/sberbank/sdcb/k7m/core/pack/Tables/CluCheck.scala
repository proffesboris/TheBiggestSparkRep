package ru.sberbank.sdcb.k7m.core.pack.Tables

import org.apache.spark.sql.SparkSession
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main}

class CluCheck(spark: SparkSession, config: Config) extends Table(spark: SparkSession) {

	override val tableName: String = "clu_check"
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

object CluCheck {
	def apply(spark: SparkSession, config: Config): CluCheck = new CluCheck(spark, config)
}