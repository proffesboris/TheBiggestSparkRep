package ru.sberbank.sdcb.k7m.core.pack.DDS.KO_LIST

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class DDS_ko_list(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(DDS_ko_list_ShortName)
	val dashboardPath: String = genDashBoardPath(DDS_ko_list_ShortName)

	val STG_ko_lst_Name: String = genDashBoardName(STG_ko_lst_ShortName)
	val STG_UL_Name: String = genDashBoardName(STG_UL_ShortName)

	val dataframe = spark.sql(
		s"""
			|
			|SELECT DISTINCT u.inn
			|FROM $STG_ko_lst_Name lst
			|INNER JOIN $STG_UL_Name u
			|ON (u.egrul_org_id=lst.egrul_org_id)
			|
			|
			|
		""".stripMargin)


}
