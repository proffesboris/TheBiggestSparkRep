package ru.sberbank.sdcb.k7m.core.pack.PRMTR1.EXEC_AGGR

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR1_exec_aggr(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR1_exec_aggr_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR1_exec_aggr_ShortName)

	val DDS_exec_Name: String= genDashBoardName(DDS_exec_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT inn -- ИНН
			|    , inn_established
			|    , fio -- ФИО
			|    , count(*) over (partition by inn, lower(fio)) as cnt_dir
			|FROM $DDS_exec_Name
		""".stripMargin)

}
