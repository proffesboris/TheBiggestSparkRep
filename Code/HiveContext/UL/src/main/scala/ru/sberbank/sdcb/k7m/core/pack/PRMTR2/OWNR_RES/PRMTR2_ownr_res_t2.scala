package ru.sberbank.sdcb.k7m.core.pack.PRMTR2.OWNR_RES

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR2_ownr_res_t2(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR2_ownr_res_t2_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR2_ownr_res_t2_ShortName)

	val PRMTR2_ownr_res_t1_Name: String= genDashBoardName(PRMTR2_ownr_res_t1_ShortName)

	val dataframe = spark.sql(
		s"""
		| select a.*, case when a.share_sum = b.max_share_sum then 1 else 0 end as max_share_flag
		| from $PRMTR2_ownr_res_t1_Name a
		| inner join
		| (
		| 		SELECT
		| 						inn_established
		| 						, max(share_sum) as max_share_sum
		| 		FROM $PRMTR2_ownr_res_t1_Name
		| 		GROUP BY inn_established
		| ) b
		| on a.inn_established = b.inn_established
		""".stripMargin)

}

