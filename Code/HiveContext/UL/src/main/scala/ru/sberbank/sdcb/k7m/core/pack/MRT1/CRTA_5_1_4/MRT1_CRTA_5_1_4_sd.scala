package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_4

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_4_sd(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_4_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_4_ShortName)

	val prmtr4_dir_mtch_Name = genDashBoardName(PRMTR4_dir_mtch_ShortName)

	val dataframe = spark.sql(
		s"""
			 |SELECT
			 |    distinct
			 |            t.inn1,  -- ИНН
			 |         --   cast(NULL as string) as nm, -- ФИО (Добавил Семен)
			 |            t.inn2,
			 |            '5.1.4sd' as criterion,
			 |            10 as confidence,
			 |            cast(t.max_share_dir_brd as decimal(22, 5)) as quantity,
			 |            '$DATE' as dt
			 |FROM $prmtr4_dir_mtch_Name t
		""".stripMargin)

}
