package ru.sberbank.sdcb.k7m.core.pack.dashboards

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import ru.sberbank.sdcb.k7m.core.pack.{AmrlirtMain, Config}

class AmrEadPrd(config: Config)  extends Table(config: Config) {

	import spark.implicits._

	val dashboardName: String = s"${config.aux}.amr_ead_prd"
	val dashboardPath = s"${config.auxPath}amr_ead_prd"

	private val pre_InnerDataframe = BaseJoinsDataframe.where(model_tables("code") === "CommonTBL_Alpha_factor_ead")

	private val InnerDataframe = pre_InnerDataframe
	  	.groupBy("mt_rows_columns2.name")
			.agg(
				max(when(col("mt_rows_columns1.name") === "ProductName", models_tables_values("value"))).alias("prd_name"),
				max(when(col("mt_rows_columns1.name") === "Product_Status", models_tables_values("value"))).alias("prd_stts"),
				max(when(col("mt_rows_columns1.name") === "ProductLimit", models_tables_values("value"))).alias("prd_lim"),
				max(when(col("mt_rows_columns1.name") === "CCFp", models_tables_values("value"))).alias("ccfp_val"),
				max(when(col("mt_rows_columns1.name") === "CCFp_other", models_tables_values("value"))).alias("ccfp_other_val"),
				max(when(col("mt_rows_columns1.name") === "CCFm", models_tables_values("value"))).alias("ccfm_val"),
				max(when(col("mt_rows_columns1.name") === "CCFm_dt", models_tables_values("value"))).alias("ccfm_dt_val")
			)

	private val dataFrameStrings: DataFrame = InnerDataframe
		.select(
			$"prd_name",
			$"prd_stts",
			$"prd_lim",
			$"ccfp_val",
			$"ccfp_other_val",
			$"ccfm_val",
			$"ccfm_dt_val"
	)

	private val ColumnsToBECasted = Seq("ccfp_val", "ccfp_other_val", "ccfm_val", "ccfm_dt_val")

	val dataFrame: DataFrame = castFieldsToDecimal(dataFrameStrings, ColumnsToBECasted).withColumn("ccfp_val", $"ccfp_val".cast(StringType))

}
