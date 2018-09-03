package ru.sberbank.sdcb.k7m.core.pack.PRMTR1.DIR_AGGR

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR1_dir_aggr(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR1_dir_aggr_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR1_dir_aggr_ShortName)

	val DDS_dir_brd_Name: String= genDashBoardName(DDS_dir_brd_ShortName)

	val dataframe = spark.sql(
	s"""
			|SELECT
			|     inn_established
			|    , fio -- ФИО
			|    , birth_year -- Год рождения
			|    , count(*) over (partition by inn_established)  as cnt_dir
			|FROM $DDS_dir_brd_Name
		""".stripMargin)

}
