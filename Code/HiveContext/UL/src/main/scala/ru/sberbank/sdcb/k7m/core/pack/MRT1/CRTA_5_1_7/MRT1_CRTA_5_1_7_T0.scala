package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_7

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_7_T0(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_7_T0_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_7_T0_ShortName)

	val Prmtr4_expownr_Name: String = genDashBoardName(PRMTR4_extpownr_ShortName)



	val dataframe = spark.sql(
		s"""
			|select distinct
			|        inn1 -- ИНН
			|      --  , nm -- ФИО (Добавил Семен)
			|        , inn2
			|        , quantity
			|        , max_share_flag
			|        , is_ownr
			|        , family_id
			|from $Prmtr4_expownr_Name
			|where (is_ownr=0) or (quantity >= 0.2 or ((quantity>=0.01) and (max_share_flag = 1)))
		""".stripMargin)


}
