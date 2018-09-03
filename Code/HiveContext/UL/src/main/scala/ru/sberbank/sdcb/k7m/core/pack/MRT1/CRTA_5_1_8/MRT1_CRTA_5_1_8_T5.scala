package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_8

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_8_T5(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_8_T5_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_8_T5_ShortName)

	val MRT1_crta_5_1_8_T1_Name = genDashBoardName(MRT1_Crta_5_1_8_T1_ShortName)
	val MRT1_crta_5_1_8_T3_Name = genDashBoardName(MRT1_Crta_5_1_8_T3_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT shares.family_id,  shares.inn2, shares.quantity
			|FROM $MRT1_crta_5_1_8_T1_Name shares
			|INNER JOIN $MRT1_crta_5_1_8_T3_Name maxes
			|ON (shares.inn2 = maxes.inn2)
			|WHERE (shares.quantity >= 0.2) OR ((shares.quantity >= maxes.max_quantity) and (shares.quantity >= 0.01))
		""".stripMargin)

}
