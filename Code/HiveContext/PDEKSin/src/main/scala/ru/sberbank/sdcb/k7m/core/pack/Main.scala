package ru.sberbank.sdcb.k7m.core.pack

import org.apache.spark.sql.SparkSession

object Main extends BaseMainClass {

	override def run(params: Map[String, String], config: Config): Unit = {
		val spark: SparkSession = SparkSession.builder
			.appName("PDEKSin")
			.enableHiveSupport()
			.getOrCreate

		//val schema_pa_p = "t_team_k7m_pa_p"
		val schema_pa_d = config.pa

		//val tables_for_pa_p = new PDEKSin(spark, schema_pa_p)
		val tables_for_pa_d = new PDEKSin(spark, schema_pa_d)

		//tables_for_pa_d.create_tables()

	}
}
