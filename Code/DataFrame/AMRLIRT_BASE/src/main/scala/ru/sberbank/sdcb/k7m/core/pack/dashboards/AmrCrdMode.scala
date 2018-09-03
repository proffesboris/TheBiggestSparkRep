package ru.sberbank.sdcb.k7m.core.pack.dashboards

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, when}
import ru.sberbank.sdcb.k7m.core.pack.{AmrlirtMain, Config}

class AmrCrdMode(config: Config)  extends Table(config: Config) {

	import spark.implicits._

	val dashboardName: String = s"${config.aux}.amr_crd_mode"
	val dashboardPath = s"${config.auxPath}amr_crd_mode"

	private val pre_InnerDataframe = BaseJoinsDataframe.where(model_tables("code") === "CommonTBL_CreditModeDecode")

	private val InnerDataframe = pre_InnerDataframe
		.groupBy(col("mt_rows_columns2.name"))
		.agg(
			max(when(col("mt_rows_columns1.name") === "CreditMode", models_tables_values("value"))).alias("crd_mode_cd"),
			max(when(col("mt_rows_columns1.name") === "CreditRiskFlag", models_tables_values("value"))).alias("crd_risk_flag"),
			max(when(col("mt_rows_columns1.name") === "Type", models_tables_values("value"))).alias("type_cd"),
			max(when(col("mt_rows_columns1.name") === "LoanSort", models_tables_values("value"))).alias("loan_sort_num"),
			max(when(col("mt_rows_columns1.name") === "CCFm", models_tables_values("value"))).alias("ccfm_val"),
			max(when(col("mt_rows_columns1.name") === "CCFm_dt", models_tables_values("value"))).alias("ccfm_dt_val"),
			max(when(col("mt_rows_columns1.name") === "Ki", models_tables_values("value"))).alias("ki_val"),
			max(when(col("mt_rows_columns1.name") === "Kf", models_tables_values("value"))).alias("kf_val")
		)

	private val dataFrameStrings: DataFrame = InnerDataframe.select(
		$"crd_mode_cd",
		$"crd_risk_flag",
		$"type_cd",
		$"loan_sort_num",
		$"ccfm_val",
		$"ccfm_dt_val",
		$"ki_val",
		$"kf_val"
	)

	private val ColumnsToBECasted = Seq("ccfm_val", "ccfm_dt_val", "ki_val", "kf_val")

	val dataFrame: DataFrame = castFieldsToDecimal(dataFrameStrings, ColumnsToBECasted)


}
