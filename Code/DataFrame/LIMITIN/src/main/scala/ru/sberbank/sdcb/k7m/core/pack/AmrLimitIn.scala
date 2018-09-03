package ru.sberbank.sdcb.k7m.core.pack

import java.time.LocalDate

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

class AmrLimitIn(val config: Config, date: LocalDate) extends Table(config: Config) {

	import spark.implicits._

	val dashboardName: String = s"${config.pa}.limitin"
	val dashboardPath = s"${config.paPath}limitin"

	private val calcReq = new AmrCalcReq(config, date)
	private val amr_calc_req: DataFrame = spark.table(calcReq.dashboardName)

	private val amr_calc_req_date_string = amr_calc_req.withColumn(
		"rqst_rep_dt", concat(substring($"rqst_rep_dt", 7, 4), lit("-"), substring($"rqst_rep_dt", 4, 2), lit("-"), substring($"rqst_rep_dt", 1, 2), lit(" 00:00:00.0"))
	)

	private val amr_calc_req_transformed = amr_calc_req_date_string.withColumn(
		"rqst_rep_dt", $"rqst_rep_dt".cast("timestamp"))


	val dataFrame: DataFrame = amr_calc_req_transformed
		.join(broadcast(clu), clu("crm_id") === amr_calc_req_transformed("crm_cust_id"), "inner")
	  	.join(broadcast(fok_fin_stmt_debt), fok_fin_stmt_debt("crm_cust_id") === amr_calc_req_transformed("crm_cust_id") &&
				fok_fin_stmt_debt("fin_stmt_rep_dt") === amr_calc_req_transformed("rqst_rep_dt"), "left")
  			.join(broadcast(fok_fin_stmt_rsbu), fok_fin_stmt_rsbu("crm_cust_id") === amr_calc_req_transformed("crm_cust_id") &&
					fok_fin_stmt_rsbu("fin_stmt_rep_dt") === amr_calc_req_transformed("rqst_rep_dt"), "left")
					.join(broadcast(fok_fin_stmt_debt_sbrf), fok_fin_stmt_debt_sbrf("crm_cust_id") === amr_calc_req_transformed("crm_cust_id") &&
						fok_fin_stmt_debt_sbrf("fin_stmt_rep_dt") === amr_calc_req_transformed("rqst_rep_dt"), "left")
		.where(
		(amr_calc_req_transformed("model_cd")
			.isin("CC01", "CC01_M_A", "CC01_price_ttc", "CC02", "CC02_price_ttc", "CC03", "CC03_price_ttc", "CC04", "CC04_price_ttc",
				"CC05", "CC06", "CC09", "CC10", "CC11", "CC11_price_ttc","CC12", "CC12_price_ttc", "CC13", "CC13_price_ttc", "CC14_price_ttc",
				"CC_IFRS", "CC_RAS", "CC_RAS_v2", "CC_RAS_v3", "CC_RAS_v4", "CC_RAS_v5", "PD_CORP_CC_1_0117_1_0117_OPK", "PD_CORP_CC_1_0117_1_0117_R",
				"PD_CORP_CC_1_0117_1_0117_R_v2", "PD_CORP_CC_1_0117_1_0117_R_v3", "PD_CORP_CC_1_0117_2_0817_OPK_1", "PD_CORP_CC_1_0117_2_0817_R_1") ||
			upper(amr_calc_req_transformed("model_cd")).like("PD_CORP_CC%")) &&
			!amr_calc_req_transformed("model_type_cd").isin("CC_OPK_REORG","CC_REORG") &&
			$"final_rqst_dttm" > add_months(lit(date.toString), -12) &&
			$"final_rqst_dttm" <= date_add(lit(date.toString), 1)
	).select(
			clu("u7m_id"),
			amr_calc_req_transformed("*"),
			exp(amr_calc_req_transformed("q101b_rev_val")).as("rev_amt"),
			when(amr_calc_req_transformed("gr_int_rating_val").isNull || amr_calc_req_transformed("gr_int_rating_val").isin("0", "-1000", "0,0"), "").otherwise(amr_calc_req_transformed("gr_int_rating_val")).alias("gr_int_rating_s_val"),
			fok_fin_stmt_debt("fin_leas_debt_amt"),
			when(
					coalesce(fok_fin_stmt_rsbu("fin_stmt_1410_amt"), lit(0)) +
					coalesce(fok_fin_stmt_rsbu("fin_stmt_1510_amt"), lit(0)) > 0 &&
					coalesce(fok_fin_stmt_rsbu("fin_stmt_1410_amt"), lit(0)) +
					coalesce(fok_fin_stmt_rsbu("fin_stmt_1510_amt"), lit(0)) -
					coalesce(fok_fin_stmt_debt("fin_crd_debt_amt"), lit(0)) -
					coalesce(fok_fin_stmt_debt_sbrf("fin_loan_debt_amt"), lit(0)) > 0,
					coalesce(fok_fin_stmt_rsbu("fin_stmt_1410_amt"), lit(0)) +
					coalesce(fok_fin_stmt_rsbu("fin_stmt_1510_amt"), lit(0)) -
					coalesce(fok_fin_stmt_debt("fin_crd_debt_amt"), lit(0)) -
					coalesce(fok_fin_stmt_debt_sbrf("fin_loan_debt_amt"), lit(0))
			).otherwise(lit(0)).cast(DecimalType(16,4)).as("fin_loan_debt_amt"),
			fok_fin_stmt_debt("fin_crd_debt_amt")
		).withColumn("rn", row_number().over(Window
			.partitionBy(amr_calc_req_transformed("crm_cust_id"))
			.orderBy(amr_calc_req_transformed("final_rqst_dttm").desc
			)
		)
	).where ($"rn" === 1)
  	.drop("rn")



}
