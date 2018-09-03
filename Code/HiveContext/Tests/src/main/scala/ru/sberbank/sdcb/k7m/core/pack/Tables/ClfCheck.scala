package ru.sberbank.sdcb.k7m.core.pack.Tables

import org.apache.spark.sql.SparkSession
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main}

class ClfCheck(spark: SparkSession, config: Config) extends Table(spark: SparkSession) {

	override val tableName: String = "clf_check"
	override val tableSchema: String = config.pa

	val fullName: String = tableSchema + "." + tableName

	override val creationQuery: String =
		s"""
			 |create table if not exists $fullName(
			 |	f7m_id string,
			 |	check_name string,
			 |	check_lvl tinyint,
			 |	check_flg tinyint,
			 |	exec_id string
			 |) stored as parquet
		 """.stripMargin

}

object ClfCheck {
	def apply(spark: SparkSession, config: Config): ClfCheck = new ClfCheck(spark, config)
}