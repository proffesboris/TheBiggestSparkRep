package ru.sberbank.sdcb.k7m.core.pack.PRMTR4.BG_OWNR

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR4_bg_ownr(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR4_bg_ownr_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR4_bg_ownr_ShortName)

	val prmrt3_ownr_ul: String = genDashBoardName(PRMTR3_ownr_ul_ShortName)
	val dds_ko_list: String = genDashBoardName(DDS_ko_list_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT o.inn1, -- ИНН
			|--o.nm, -- ФИО (Добавил Семен)
			|o.inn2
			|FROM $prmrt3_ownr_ul o
			|INNER JOIN  $dds_ko_list lst
			|ON (o.inn1 = lst.inn)
			|WHERE (o.share_sum >= 0.2) or (o.max_share_flag = 1 and o.share_sum>=0.01)
		""".stripMargin)

}
