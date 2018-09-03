package ru.sberbank.sdcb.k7m.core.pack.STG.KO_LST

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class STG_KO_Lst(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_ko_lst_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_ko_lst_ShortName)

	val dataframe = spark.sql(
		s"""
			|
			|SELECT
			|    DISTINCT la.egrul_org_id
			|FROM $ZeroLayerSchema.int_license_activity la
			|WHERE lower(la.la_activity_nm) like "%банк%"
			|    AND not lower(la.la_activity_nm) like "%страховая%"
			|    AND not lower(la.la_activity_nm) like "%отдельные виды деятельности%"
			|    AND not lower(la.la_activity_nm) like "%прекращен%"
			|
			|
		""".stripMargin)

}
