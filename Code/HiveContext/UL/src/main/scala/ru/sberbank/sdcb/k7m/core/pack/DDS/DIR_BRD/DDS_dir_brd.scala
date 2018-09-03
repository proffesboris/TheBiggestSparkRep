package ru.sberbank.sdcb.k7m.core.pack.DDS.DIR_BRD

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class DDS_dir_brd(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(DDS_dir_brd_ShortName)
	val dashboardPath: String = genDashBoardPath(DDS_dir_brd_ShortName)

	val Stg_dir_brd_Name: String = genDashBoardName(STG_dir_brd_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT
			|    bd.bd_ul_inn as inn_established,
			|    bd.bd_fio as fio, -- ФИО
			|    bd.bd_post_nm, -- Должность
			|    bd.bd_birth_dt as birth_year -- Год рождения
			|FROM $Stg_dir_brd_Name bd
			|
		""".stripMargin)

}
