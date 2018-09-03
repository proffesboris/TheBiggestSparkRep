package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_8

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_8_T1(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_8_T1_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_8_T1_ShortName)

	val prmtr4_extpownr_Name = genDashBoardName(PRMTR4_extpownr_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT
			|        t.family_id
			|        , t.inn2
			|        , sum(t.quantity) as quantity
			|FROM $prmtr4_extpownr_Name t
			|where (t.is_ownr = 1) and (t.family_id is not null)
			|GROUP BY t.family_id, t.inn2
		""".stripMargin)


}
