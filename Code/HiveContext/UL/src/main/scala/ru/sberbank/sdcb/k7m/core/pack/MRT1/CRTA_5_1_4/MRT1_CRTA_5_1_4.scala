package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_4

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_4(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_4_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_4_ShortName)

	val prmtr3_ownr_ul_Name = genDashBoardName(PRMTR3_ownr_ul_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT
			|    distinct
			|            t.inn1, -- ИНН
			|           -- t.nm, -- ФИО (Добавил Семен)
			|            t.inn2,
			|            '5.1.4' as criterion,
			|            100 as confidence,
			|            t.quantity as quantity,
			|            '$DATE' as dt
			|FROM
			|    (
			|        SELECT
			|                inn1 -- ИНН
			|             -- ,  nm -- ФИО (Добавил Семен)
			|                , inn2
			|                , share_sum as quantity
			|        FROM $prmtr3_ownr_ul_Name
			|        WHERE (share_sum < 0.5) and ((share_sum >= 0.2) or (max_share_flag = 1 and share_sum>=0.01))
			|    ) t
		""".stripMargin)

}
