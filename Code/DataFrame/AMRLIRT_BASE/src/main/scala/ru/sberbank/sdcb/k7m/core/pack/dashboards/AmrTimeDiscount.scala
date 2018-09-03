package ru.sberbank.sdcb.k7m.core.pack.dashboards

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ru.sberbank.sdcb.k7m.core.pack.{AmrlirtMain, Config}

class AmrTimeDiscount(config: Config)  extends Table(config: Config){

	import spark.implicits._

	val dashboardName: String = s"${config.aux}.amr_time_discount"
	val dashboardPath = s"${config.auxPath}amr_time_discount"

	private val pre_InnerDataframe = BaseJoinsDataframe.where(model_tables("code") === "CommonTBL_KTimeDisc")

	private val inner = pre_InnerDataframe.groupBy(col("mt_rows_columns2.name")).agg(
		max(when(col("mt_rows_columns1.name") === "MaxTerm", models_tables_values("value"))).alias("max_term"),
		max(when(col("mt_rows_columns1.name") === "MinTerm", models_tables_values("value"))).alias("min_term"),
		max(when(col("mt_rows_columns1.name") === "Kmax", models_tables_values("value"))).alias("kmax"),
		max(when(col("mt_rows_columns1.name") === "Kmin", models_tables_values("value"))).alias("kmin")
	)

	private val dataFrameStrings: DataFrame = inner.select(
		$"min_term",
		$"max_term",
		$"kmin",
		$"kmax"
	)

	private val ColumnsToBECasted = Seq("kmin", "kmax", "min_term", "max_term")

	val dataFrame: DataFrame = castFieldsToDecimal(dataFrameStrings, ColumnsToBECasted)

}
