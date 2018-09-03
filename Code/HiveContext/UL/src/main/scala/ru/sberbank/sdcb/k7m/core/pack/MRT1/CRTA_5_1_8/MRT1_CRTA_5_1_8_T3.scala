package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_8

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_8_T3(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_8_T3_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_8_T3_ShortName)

	val prmtr3_ownr_ul_Name = genDashBoardName(PRMTR3_ownr_ul_ShortName)
	val prmtr3_ownr_fl_Name = genDashBoardName(PRMTR3_ownr_fl_ShortName)

	val dataframe = spark.sql(
		s"""
			 |SELECT t.inn2, max(t.share_sum) as max_quantity
			 |FROM
			 |    (
			 |        SELECT inn2, max(share_sum) as share_sum
			 |        FROM $prmtr3_ownr_ul_Name
			 |        GROUP BY inn2
			 |    UNION ALL
			 |        SELECT inn2, max(share_sum) as share_sum
			 |        FROM $prmtr3_ownr_fl_Name
			 |        GROUP BY inn2
			 |    ) t
			 |GROUP BY inn2
		""".stripMargin)

}
