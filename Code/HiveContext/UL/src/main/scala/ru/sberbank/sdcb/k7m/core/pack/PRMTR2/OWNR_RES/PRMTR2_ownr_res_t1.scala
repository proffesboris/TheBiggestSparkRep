package ru.sberbank.sdcb.k7m.core.pack.PRMTR2.OWNR_RES

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR2_ownr_res_t1(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR2_ownr_res_t1_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR2_ownr_res_t1_ShortName)

	val PRMTR1_ownr_rec_Name: String= genDashBoardName(PRMTR1_ownr_rec_ShortName)

	val dataframe = spark.sql(
		s"""
			|    SELECT
			|            inn -- ИНН
			|            , inn_established
			|            , sum(share_sum) as share_sum
			|            , share_type
			|    FROM $PRMTR1_ownr_rec_Name
			|    GROUP BY inn, inn_established, share_type
		""".stripMargin)

}
