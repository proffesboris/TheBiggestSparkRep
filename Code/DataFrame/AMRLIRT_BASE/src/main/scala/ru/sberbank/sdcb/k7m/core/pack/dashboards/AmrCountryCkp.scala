package ru.sberbank.sdcb.k7m.core.pack.dashboards

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ru.sberbank.sdcb.k7m.core.pack.{AmrlirtMain, Config}

class AmrCountryCkp(config: Config) extends Table(config: Config) {

	import spark.implicits._

	val dashboardName: String =  s"${config.aux}.amr_country_ckp"
	val dashboardPath = s"${config.auxPath}amr_country_ckp"

	private val pre_InnerDataframe = BaseJoinsDataframe.where(model_tables("code") === "CommonTBL_CountryCKP")

	private val InnerDataframe = pre_InnerDataframe
		.groupBy(col("mt_rows_columns2.name"))
		.agg(
			max(when(col("mt_rows_columns1.name") === "CountryCode", models_tables_values("value"))).alias("country_cd"),
			max(when(col("mt_rows_columns1.name") === "LGTEValue", models_tables_values("value"))).alias("lgte_val"),
			max(when(col("mt_rows_columns1.name") === "PTEValue", models_tables_values("value"))).alias("pte_val"),
			max(when(col("mt_rows_columns1.name") === "CurrencyCode", models_tables_values("value"))).alias("currency_cd")
		)

	private val dataFrameStrings = InnerDataframe.select(
		$"country_cd",
		$"lgte_val",
		$"pte_val",
		$"currency_cd"
	).where($"country_cd".isNotNull)

	private val ColumnsToBECasted = Seq("pte_val", "lgte_val")

	val dataFrame: DataFrame = castFieldsToDecimal(dataFrameStrings, ColumnsToBECasted)

}
