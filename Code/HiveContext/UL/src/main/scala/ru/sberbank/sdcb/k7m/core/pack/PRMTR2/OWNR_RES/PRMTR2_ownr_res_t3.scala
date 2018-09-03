package ru.sberbank.sdcb.k7m.core.pack.PRMTR2.OWNR_RES

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR2_ownr_res_t3(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR2_ownr_res_t3_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR2_ownr_res_t3_ShortName)

	val PRMTR2_ownr_res_t2_Name: String= genDashBoardName(PRMTR2_ownr_res_t2_ShortName)

	val dataframe = spark.sql(
		s"""
			 |select
			 |        a.inn
			 |        ,a.inn_established
			 |        ,a.share_sum
			 |        ,a.share_type
			 |        , case when b.count_max_share_flag > 1 then 0 else a.max_share_flag end as max_share_flag
			 |from $PRMTR2_ownr_res_t2_Name a
			 |inner join
			 |(
			 |    SELECT
			 |            inn_established
			 |            , count(max_share_flag) as count_max_share_flag
			 |    FROM $PRMTR2_ownr_res_t2_Name
			 |    GROUP BY inn_established
			 |) b
			 |on a.inn_established = b.inn_established
		""".stripMargin)

}