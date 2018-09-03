package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class AmrCalcReq(val config: Config, date: LocalDate) extends Table(config: Config) {

	import spark.implicits._

	val dashboardName = s"${config.aux}.amr_calc_req"
	val dashboardPath = s"${config.auxPath}amr_calc_req"

	private val requests1 = spark.table(s"$source.amr_requests").alias("requests1")
	private val requests2 = spark.table(s"$source.amr_requests").alias("requests2")
	private val pd_rating_final = spark.table(s"$source.amr_pd_rating_final")
	private val ratings = spark.table(s"$source.amr_ratings")
	private val stage_data = spark.table(s"$source.amr_stage_data")
	private val model_parameters = spark.table(s"$source.amr_model_parameters")
	private val request_data = spark.table(s"$source.amr_request_data")
	private val organizations = spark.table(s"$source.amr_organizations")
	private val coborrower_groups = spark.table(s"$source.amr_coborrower_groups")
	private val requestors = spark.table(s"$source.amr_requestors")

	private val pre_fin_calc = requests1
		.join(broadcast(requestors), col("requests1.requestor_id") === requestors("requestor_id"), "inner")
		.join(broadcast(requests2), col("requests1.rating_identifier") === col("requests2.rating_identifier"), "inner")
  	.select(
			col("requests1.request_id").alias("final_request_id"),
			col("requests1.request_dt").alias("final_request_dt"),
			col("requests1.model_integration_uid").alias("model_cd"),
			col("requests1.rating_identifier").alias("rating_id"),
			col("requests2.request_id").alias("calc_request_id")
		).where(
			col("requests1.rating_identifier").isNotNull &&
			col("requests1.request_type_id") === 5 &&
			col("requests1.is_process_failed") === "N" &&
			col("requests2.request_type_id").isin(2, 3) &&
			col("requests2.is_process_failed") === "N" &&
			col("requests2.request_dt").leq(requests1("request_dt"))
		)

	private val fin_calc = pre_fin_calc
		.groupBy(
			$"final_request_id",
			$"final_request_dt",
			$"model_cd",
			$"rating_id"
		).agg(
			max($"calc_request_id").alias("calc_request_id")
		)

	private val org_calc = fin_calc
		.join(broadcast(requests1), fin_calc("calc_request_id") === requests1("request_id"))
		.join(broadcast(organizations), requests1("organization_id") === organizations("organization_id"), "left")
		.join(broadcast(coborrower_groups), requests1("coborrower_group_id") === coborrower_groups("coborrower_group_id"), "left")
		.select(
			fin_calc("final_request_id"),
			fin_calc("final_request_dt"),
			fin_calc("model_cd"),
			fin_calc("rating_id"),
			fin_calc("calc_request_id"),
			organizations("inn").alias("org_inn"),
			organizations("kpp").alias("org_kpp"),
			requests1("organization_id"),
			coborrower_groups("code"),
			organizations("code"),
			when(requests1("organization_id").isNull, coborrower_groups("code")).otherwise(organizations("code")).alias("crm_cust_id"),
			when(requests1("organization_id").isNull, upper(coborrower_groups("name"))).otherwise(upper(organizations("name"))).alias("org_name"),
			when(requests1("organization_id").isNull, "GSZ_ID").otherwise("ORG_ID").alias("qsz_org")
		).where(
		requests1("organization_id").isNotNull ||
		requests1("coborrower_group_id").isNotNull
	)

	private val pre_dataFrame = org_calc
		.join(broadcast(pd_rating_final), pd_rating_final("request_id") === org_calc("final_request_id"))
		.join(broadcast(ratings), pd_rating_final("rating_id") === ratings("rating_id"))
		.join(stage_data, org_calc("calc_request_id") === stage_data("request_id") && stage_data("stage_id") === 3)
		.join(broadcast(model_parameters), stage_data("model_parameter_id") === model_parameters("model_parameter_id"))
		.join(request_data, stage_data("model_parameter_id") === request_data("model_parameter_id") && stage_data("request_id") === request_data("request_id"), "left")

	val dataFrameStrings: DataFrame = pre_dataFrame
		.groupBy(
			org_calc("final_request_id").alias("final_rqst_id"),
			org_calc("final_request_dt").alias("final_rqst_dttm"),
			org_calc("model_cd"),
			org_calc("rating_id").alias("crm_rating_card_id"),
			org_calc("calc_request_id").alias("calc_rqst_id"),
			ratings("code").alias("cust_rating_cd"),
			org_calc("crm_cust_id"),
			org_calc("org_name").alias("crm_cust_name"),
			org_calc("org_inn").alias("cust_inn_num"),
			org_calc("org_kpp").alias("cust_kpp_num")
		).agg(
			max(request_data("report_date")).alias("rqst_rep_dt"),
			max(request_data("start_date_str")).alias("rqst_rep_start_dt"),
			max(request_data("end_date_str")).alias("rqst_rep_end_dt"),
			max(when(lower(model_parameters("code")) === "asset_q1", stage_data("parameter_value"))).alias("asset_q1_amt"),
			max(when(lower(model_parameters("code")) === "rev_q1", stage_data("parameter_value"))).alias("rev_q1_amt"),
			max(when(lower(model_parameters("code")) === "rev_q2", stage_data("parameter_value"))).alias("rev_q2_amt"),
			max(when(lower(model_parameters("code")) === "rev_y", stage_data("parameter_value"))).alias("rev_y_amt"),
			max(when(lower(model_parameters("code")) === "calc_ebitda", stage_data("parameter_value"))).alias("calc_ebitda_amt"),
			max(when(lower(model_parameters("code")) === "short_debt_q1", stage_data("parameter_value"))).alias("short_debt_q1_amt"),
			max(when(lower(model_parameters("code")) === "long_debt_q1", stage_data("parameter_value"))).alias("long_debt_q1_amt"),
			max(when(lower(model_parameters("code")) === "cor_debt_q1", stage_data("parameter_value"))).alias("cor_debt_q1_amt"),
			max(when(lower(model_parameters("code")) === "short_debt_q2", stage_data("parameter_value"))).alias("short_debt_q2_amt"),
			max(when(lower(model_parameters("code")) === "long_debt_q2", stage_data("parameter_value"))).alias("long_debt_q2_amt"),
			max(when(lower(model_parameters("code")) === "cor_debt_q2", stage_data("parameter_value"))).alias("cor_debt_q2_amt"),
			max(when(lower(model_parameters("code")) === "model_type", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("model_type_cd"),
			max(when(lower(model_parameters("code")) === "ql1", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql1_val"),
			max(when(lower(model_parameters("code")) === "ql2", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql2_val"),
			max(when(lower(model_parameters("code")) === "ql3", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql3_val"),
			max(when(lower(model_parameters("code")) === "ql4", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql4_val"),
			max(when(lower(model_parameters("code")) === "ql5", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql5_val"),
			max(when(lower(model_parameters("code")) === "ql6", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql6_val"),
			max(when(lower(model_parameters("code")) === "ql7", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql7_val"),
			max(when(lower(model_parameters("code")) === "ql8", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql8_val"),
			max(when(lower(model_parameters("code")) === "ql9", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql9_val"),
			max(when(lower(model_parameters("code")) === "ql10", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql10_val"),
			max(when(lower(model_parameters("code")) === "ql11", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql11_val"),
			max(when(lower(model_parameters("code")) === "ql12", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql12_val"),
			max(when(lower(model_parameters("code")) === "ql13", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql13_val"),
			max(when(lower(model_parameters("code")) === "ql14", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql14_val"),
			max(when(lower(model_parameters("code")) === "ql15", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql15_val"),
			max(when(lower(model_parameters("code")) === "ql16", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql16_val"),
			max(when(lower(model_parameters("code")) === "ql17", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql17_val"),
			max(when(lower(model_parameters("code")) === "ql18", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql18_val"),
			max(when(lower(model_parameters("code")) === "ql19", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql19_val"),
			max(when(lower(model_parameters("code")) === "ql20", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql20_val"),
			max(when(lower(model_parameters("code")) === "ql21", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql21_val"),
			max(when(lower(model_parameters("code")) === "ql22", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql22_val"),
			max(when(lower(model_parameters("code")) === "ql23", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql23_val"),
			max(when(lower(model_parameters("code")) === "ql24", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql24_val"),
			max(when(lower(model_parameters("code")) === "ql25", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql25_val"),
			max(when(lower(model_parameters("code")) === "ql26", coalesce(request_data("value_integration_uid"), stage_data("parameter_value")))).alias("ql26_val"),
			max(when(lower(model_parameters("code")) === "ql11_score", stage_data("parameter_value"))).alias("ql11_score_val"),
			max(when(lower(model_parameters("code")) === "ql13_score", stage_data("parameter_value"))).alias("ql13_score_val"),
			max(when(lower(model_parameters("code")) === "ql15_score", stage_data("parameter_value"))).alias("ql15_score_val"),
			max(when(lower(model_parameters("code")) === "ql16_score", stage_data("parameter_value"))).alias("ql16_score_val"),
			max(when(lower(model_parameters("code")) === "ql3_score", stage_data("parameter_value"))).alias("ql3_score_val"),
			max(when(lower(model_parameters("code")) === "ql5_score", stage_data("parameter_value"))).alias("ql5_score_val"),
			max(when(lower(model_parameters("code")) === "ql9_score", stage_data("parameter_value"))).alias("ql9_score_val"),
			max(when(lower(model_parameters("code")) === "woe_ql15", stage_data("parameter_value"))).alias("woe_ql15_val"),
			max(when(lower(model_parameters("code")) === "woe_ql16", stage_data("parameter_value"))).alias("woe_ql16_val"),
			max(when(lower(model_parameters("code")) === "woe_ql5", stage_data("parameter_value"))).alias("woe_ql5_val"),
			max(when(lower(model_parameters("code")) === "woe_ql7", stage_data("parameter_value"))).alias("woe_ql7_val"),
			max(when(lower(model_parameters("code")) === "woe_ql9", stage_data("parameter_value"))).alias("woe_ql9_val"),
			max(when(lower(model_parameters("code")) === "ws1" || upper(model_parameters("code")).like("DT%_WS1"), stage_data("parameter_value"))).alias("ws1_val"),
			max(when(lower(model_parameters("code")) === "ws2"|| upper(model_parameters("code")).like("DT%_WS2"), stage_data("parameter_value"))).alias("ws2_val"),
			max(when(lower(model_parameters("code")) === "ws3"|| upper(model_parameters("code")).like("DT%_WS3"), stage_data("parameter_value"))).alias("ws3_val"),
			max(when(lower(model_parameters("code")) === "ws4"|| upper(model_parameters("code")).like("DT%_WS4"), stage_data("parameter_value"))).alias("ws4_val"),
			max(when(lower(model_parameters("code")) === "ws5"|| upper(model_parameters("code")).like("DT%_WS5"), stage_data("parameter_value"))).alias("ws5_val"),
			max(when(lower(model_parameters("code")) === "ws6"|| upper(model_parameters("code")).like("DT%_WS6"), stage_data("parameter_value"))).alias("ws6_val"),
			max(when(lower(model_parameters("code")) === "ws7"|| upper(model_parameters("code")).like("DT%_WS7"), stage_data("parameter_value"))).alias("ws7_val"),
			max(when(lower(model_parameters("code")) === "ws7_1"|| upper(model_parameters("code")).like("DT%_WS7_1"), stage_data("parameter_value"))).alias("ws7_1_val"),
			max(when(lower(model_parameters("code")) === "ws8"|| upper(model_parameters("code")).like("DT%_WS8"), stage_data("parameter_value"))).alias("ws8_val"),
			max(when(lower(model_parameters("code")) === "ws9"|| upper(model_parameters("code")).like("DT%_WS9"), stage_data("parameter_value"))).alias("ws9_val"),
			max(when(lower(model_parameters("code")) === "ws10"|| upper(model_parameters("code")).like("DT%_WS10"), stage_data("parameter_value"))).alias("ws10_val"),
			max(when(lower(model_parameters("code")) === "ws10_1"|| upper(model_parameters("code")).like("DT%_WS10_1"), stage_data("parameter_value"))).alias("ws10_1_val"),
			max(when(lower(model_parameters("code")) === "ws11"|| upper(model_parameters("code")).like("DT%_WS11"), stage_data("parameter_value"))).alias("ws11_val"),
			max(when(lower(model_parameters("code")) === "ws12"|| upper(model_parameters("code")).like("DT%_WS12"), stage_data("parameter_value"))).alias("ws12_val"),
			max(when(lower(model_parameters("code")) === "ws13"|| upper(model_parameters("code")).like("DT%_WS13"), stage_data("parameter_value"))).alias("ws13_val"),
			max(when(lower(model_parameters("code")) === "ws13_1"|| upper(model_parameters("code")).like("DT%_WS13_1"), stage_data("parameter_value"))).alias("ws13_1_val"),
			max(when(lower(model_parameters("code")) === "ws14"|| upper(model_parameters("code")).like("DT%_WS14"), stage_data("parameter_value"))).alias("ws14_val"),
			max(when(lower(model_parameters("code")) === "ws14_1"|| upper(model_parameters("code")).like("DT%_WS14_1"), stage_data("parameter_value"))).alias("ws14_1_val"),
			max(when(lower(model_parameters("code")) === "ws15"|| upper(model_parameters("code")).like("DT%_WS15"), stage_data("parameter_value"))).alias("ws15_val"),
			max(when(lower(model_parameters("code")) === "ws16"|| upper(model_parameters("code")).like("DT%_WS16"), stage_data("parameter_value"))).alias("ws16_val"),
			max(when(lower(model_parameters("code")) === "ws17"|| upper(model_parameters("code")).like("DT%_WS17"), stage_data("parameter_value"))).alias("ws17_val"),
			max(when(lower(model_parameters("code")) === "ws18"|| upper(model_parameters("code")).like("DT%_WS18"), stage_data("parameter_value"))).alias("ws18_val"),
			max(when(lower(model_parameters("code")) === "ws18_all"|| upper(model_parameters("code")).like("DT%_WS18_ALL"), stage_data("parameter_value"))).alias("ws18_all_val"),
			max(when(lower(model_parameters("code")) === "ws19"|| upper(model_parameters("code")).like("DT%_WS19"), stage_data("parameter_value"))).alias("ws19_val"),
			max(when(lower(model_parameters("code")) === "ws20"|| upper(model_parameters("code")).like("DT%_WS20"), stage_data("parameter_value"))).alias("ws20_val"),
			max(when(lower(model_parameters("code")) === "ws21"|| upper(model_parameters("code")).like("DT%_WS21"), stage_data("parameter_value"))).alias("ws21_val"),
			max(when(lower(model_parameters("code")) === "ws22"|| upper(model_parameters("code")).like("DT%_WS22"), stage_data("parameter_value"))).alias("ws22_val"),
			max(when(lower(model_parameters("code")) === "ws23"|| upper(model_parameters("code")).like("DT%_WS23"), stage_data("parameter_value"))).alias("ws23_val"),
			max(when(lower(model_parameters("code")) === "ws24"|| upper(model_parameters("code")).like("DT%_WS24"), stage_data("parameter_value"))).alias("ws24_val"),
			max(when(lower(model_parameters("code")) === "ws25"|| upper(model_parameters("code")).like("DT%_WS25"), stage_data("parameter_value"))).alias("ws25_val"),
			max(when(lower(model_parameters("code")) === "ws26"|| upper(model_parameters("code")).like("DT%_WS26"), stage_data("parameter_value"))).alias("ws26_val"),
			max(when(lower(model_parameters("code")) === "ws27"|| upper(model_parameters("code")).like("DT%_WS27"), stage_data("parameter_value"))).alias("ws27_val"),
			max(when(lower(model_parameters("code")) === "ws28"|| upper(model_parameters("code")).like("DT%_WS28"), stage_data("parameter_value"))).alias("ws28_val"),
			max(when(lower(model_parameters("code")) === "ws29"|| upper(model_parameters("code")).like("DT%_WS29"), stage_data("parameter_value"))).alias("ws29_val"),
			max(when(lower(model_parameters("code")) === "ws30"|| upper(model_parameters("code")).like("DT%_WS30"), stage_data("parameter_value"))).alias("ws30_val"),
			max(when(lower(model_parameters("code")) === "ws31"|| upper(model_parameters("code")).like("DT%_WS31"), stage_data("parameter_value"))).alias("ws31_val"),
			max(when(lower(model_parameters("code")) === "ws32"|| upper(model_parameters("code")).like("DT%_WS32"), stage_data("parameter_value"))).alias("ws32_val"),
			max(when(lower(model_parameters("code")) === "ws33"|| upper(model_parameters("code")).like("DT%_WS33"), stage_data("parameter_value"))).alias("ws33_val"),
			max(when(lower(model_parameters("code")) === "ws34"|| upper(model_parameters("code")).like("DT%_WS34"), stage_data("parameter_value"))).alias("ws34_val"),
			max(when(lower(model_parameters("code")) === "ws35"|| upper(model_parameters("code")).like("DT%_WS35"), stage_data("parameter_value"))).alias("ws35_val"),
			max(when(lower(model_parameters("code")) === "ws36"|| upper(model_parameters("code")).like("DT%_WS36"), stage_data("parameter_value"))).alias("ws36_val"),
			max(when(lower(model_parameters("code")) === "ws37"|| upper(model_parameters("code")).like("DT%_WS37"), stage_data("parameter_value"))).alias("ws37_val"),
			max(when(lower(model_parameters("code")) === "ws38"|| upper(model_parameters("code")).like("DT%_WS38"), stage_data("parameter_value"))).alias("ws38_val"),
			max(when(lower(model_parameters("code")) === "ws39"|| upper(model_parameters("code")).like("DT%_WS39"), stage_data("parameter_value"))).alias("ws39_val"),
			max(when(lower(model_parameters("code")) === "ws40"|| upper(model_parameters("code")).like("DT%_WS40"), stage_data("parameter_value"))).alias("ws40_val"),
			max(when(lower(model_parameters("code")) === "ws41"|| upper(model_parameters("code")).like("DT%_WS41"), stage_data("parameter_value"))).alias("ws41_val"),
			max(when(lower(model_parameters("code")) === "ws42"|| upper(model_parameters("code")).like("DT%_WS42"), stage_data("parameter_value"))).alias("ws42_val"),
			max(when(lower(model_parameters("code")) === "ws43"|| upper(model_parameters("code")).like("DT%_WS43"), stage_data("parameter_value"))).alias("ws43_val"),
			max(when(lower(model_parameters("code")) === "ws44"|| upper(model_parameters("code")).like("DT%_WS44"), stage_data("parameter_value"))).alias("ws44_val"),
			max(when(model_parameters("code") === "DT0700_1", stage_data("parameter_value"))).alias("dt0700_1_val"),
			max(when(model_parameters("code") === "DT0700_10", stage_data("parameter_value"))).alias("dt0700_10_val"),
			max(when(model_parameters("code") === "DT0700_2", stage_data("parameter_value"))).alias("dt0700_2_val"),
			max(when(model_parameters("code") === "DT0700_3", stage_data("parameter_value"))).alias("dt0700_3_val"),
			max(when(model_parameters("code") === "DT0700_4", stage_data("parameter_value"))).alias("dt0700_4_val"),
			max(when(model_parameters("code") === "DT0700_5", stage_data("parameter_value"))).alias("dt0700_5_val"),
			max(when(model_parameters("code") === "DT0700_6", stage_data("parameter_value"))).alias("dt0700_6_val"),
			max(when(model_parameters("code") === "DT0700_7", stage_data("parameter_value"))).alias("dt0700_7_val"),
			max(when(model_parameters("code") === "DT0700_8", stage_data("parameter_value"))).alias("dt0700_8_val"),
			max(when(model_parameters("code") === "DT0700_9", stage_data("parameter_value"))).alias("dt0700_9_val"),
			max(when(model_parameters("code") === "DT_1200", stage_data("parameter_value"))).alias("dt_1200_val"),
			max(when(model_parameters("code") === "DT0710", stage_data("parameter_value"))).alias("dt0710_val"),
			max(when(model_parameters("code") === "DT0100", stage_data("parameter_value"))).alias("dt0100_val"),
			max(when(model_parameters("code") === "DT0200", stage_data("parameter_value"))).alias("dt0200_val"),
			max(when(model_parameters("code") === "DT0300", stage_data("parameter_value"))).alias("dt0300_val"),
			max(when(model_parameters("code") === "DT0400", stage_data("parameter_value"))).alias("dt0400_val"),
			max(when(model_parameters("code") === "DT0500", stage_data("parameter_value"))).alias("dt0500_val"),
			max(when(model_parameters("code") === "DT0600", stage_data("parameter_value"))).alias("dt0600_val"),
			max(when(model_parameters("code") === "DT0800", stage_data("parameter_value"))).alias("dt0800_val"),
			max(when(model_parameters("code") === "DT0900", stage_data("parameter_value"))).alias("dt0900_val"),
			max(when(model_parameters("code") === "DT1000", stage_data("parameter_value"))).alias("dt1000_val"),
			max(when(model_parameters("code") === "DT1100", stage_data("parameter_value"))).alias("dt1100_val"),
			max(when(model_parameters("code") === "DT0700", stage_data("parameter_value"))).alias("dt0700_val"),
			max(when(model_parameters("code") === "DT1200", stage_data("parameter_value"))).alias("dt1200_val"),
			max(when(lower(model_parameters("code")) === "rating_price", stage_data("parameter_value"))).alias("rating_price_cd"),
			max(when(lower(model_parameters("code")) === "rating_rezerv", stage_data("parameter_value"))).alias("rating_rezerv_cd"),
			max(when(lower(model_parameters("code")) === "rgg", stage_data("parameter_value"))).alias("rgg_val"),
			max(when(lower(model_parameters("code")) === "rgg_price", stage_data("parameter_value"))).alias("rgg_price_val"),
			max(when(lower(model_parameters("code")) === "rw", stage_data("parameter_value"))).alias("rw_val"),
			max(when(lower(model_parameters("code")) === "rw_price", stage_data("parameter_value"))).alias("rw_price_val"),
			max(when(lower(model_parameters("code")) === "tsqual", stage_data("parameter_value"))).alias("tsqual_val"),
			max(when(lower(model_parameters("code")) === "tsquant", stage_data("parameter_value"))).alias("tsquant_val"),
			max(when(lower(model_parameters("code")) === "gr_ext_rating", stage_data("parameter_value"))).alias("gr_ext_rating_val"),
			max(when(lower(model_parameters("code")) === "gr_int_rating", stage_data("parameter_value"))).alias("gr_int_rating_val"),
			max(when(lower(model_parameters("code")) === "gr_int_rating_price", stage_data("parameter_value"))).alias("gr_int_rating_price_val"),
			max(when(lower(model_parameters("code")) === "q101b_rev", stage_data("parameter_value"))).alias("q101b_rev_val")
		)


	private val ColumnsToBeCasted = Seq("asset_q1_amt", "rev_q1_amt", "rev_q2_amt",
																		  "rev_y_amt", "calc_ebitda_amt", "short_debt_q1_amt",
																		  "long_debt_q1_amt", "cor_debt_q1_amt", "short_debt_q2_amt",
																			"long_debt_q2_amt", "cor_debt_q2_amt")

	private val ColumnsToDouble = Seq("q101b_rev_val")

	val pre_final_dataFrame: DataFrame = castFieldsToDecimal(dataFrameStrings, ColumnsToBeCasted)

	val dataFrame: DataFrame = castFieldsToDouble(pre_final_dataFrame, ColumnsToDouble)

}
