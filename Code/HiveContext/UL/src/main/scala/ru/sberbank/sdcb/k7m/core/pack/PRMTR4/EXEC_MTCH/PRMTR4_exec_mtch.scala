package ru.sberbank.sdcb.k7m.core.pack.PRMTR4.EXEC_MTCH

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR4_exec_mtch(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR4_exec_mtch_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR4_exec_mtch_ShortName)

	val prmrt1_exec_aggr: String = genDashBoardName(PRMTR1_exec_aggr_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT distinct
			|    e1.inn_established as inn1,
			|    e2.inn_established as inn2,
			|    e1.inn as fl_inn, -- ИНН
			|    e1.fio as fl_fio, -- ФИО (Добавил Семен)
			|    e1.cnt_dir
			|FROM $prmrt1_exec_aggr e1
			|
			|INNER JOIN $prmrt1_exec_aggr e2
			|    ON (
			|            lower(e1.fio) = lower(e2.fio)
			|            AND (e1.inn = e2.inn)
			|            and (e1.inn_established <> e2.inn_established)
			|        )
			|where e1.cnt_dir < $GEN_DIR_COMP_COUNT
		""".stripMargin)

}
