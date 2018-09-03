package ru.sberbank.sdcb.k7m.core.pack.dashboards

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{AmrlirtMain, Config}

class AmrLrParam(config: Config)  extends Table(config: Config){

	import spark.implicits._

	val dashboardName: String = s"${config.aux}.amr_lr_param"
	val dashboardPath = s"${config.auxPath}amr_lr_param"

	private val pre_InnerDataframe = BaseJoinsDataframe.where(model_tables("code") === "CommonTBL_LR2017_Parameters")

	private val inner = pre_InnerDataframe.groupBy(col("mt_rows_columns2.name")).agg(
		max(when(col("mt_rows_columns1.name") === "Name", models_tables_values("value"))).alias("param_name"),
		max(when(col("mt_rows_columns1.name") === "StartDate", models_tables_values("value"))).alias("start_dt"),
		max(when(col("mt_rows_columns1.name") === "EndDate", models_tables_values("value"))).alias("end_dt"),
		max(when(col("mt_rows_columns1.name") === "Value", models_tables_values("value"))).alias("param_val")
	)

	private val dataFrameStrings: DataFrame = inner.select(
		$"param_name",
		$"start_dt",
		$"end_dt",
		$"param_val"
	)

	private val ColumnsToBECasted = Seq("param_val")

	val dataFrame: DataFrame = castFieldsToDecimal(dataFrameStrings, ColumnsToBECasted)

}
