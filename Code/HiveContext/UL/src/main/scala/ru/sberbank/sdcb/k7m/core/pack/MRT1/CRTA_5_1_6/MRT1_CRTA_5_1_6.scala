package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_6

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_6(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_6_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_6_ShortName)

	val prmtr1_exec_aggr_Name = genDashBoardName(PRMTR1_exec_aggr_ShortName)
	val prmtr4_exec_mtch_Name = genDashBoardName(PRMTR4_exec_mtch_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT DISTINCT
			|            t.inn as inn1, -- ИНН
			|            t.fio, -- ФИО (Добавил Семен)
			|             t.inn_established as inn2,
			|            '5.1.6tp' as criterion,
			|             100 as confidence,
			|             cast(cnt_dir as decimal(22, 5)) as quantity,
			|             '$DATE' as dt
			|    from $prmtr1_exec_aggr_Name t
		""".stripMargin)

	val appened_dataframe = spark.sql(
		s"""
			|SELECT DISTINCT
			|            t.inn1,
			|            t.fl_fio as fio -- ФИО (Добавил Семен, но тут вообще не уверен что так надо, спросить у Павла)
			|            , t.inn2
			|            , '5.1.6' as criterion
			|            , 100 as confidence
			|            , cast(0 as decimal(22, 5)) as quantity
			|            , '$DATE' as dt
			|    from $prmtr4_exec_mtch_Name t
		""".stripMargin)

	def create(): Unit = {
		save()
		appened_dataframe.write
			.format("parquet")
			.option("path", dashboardPath)
			.mode("append")
			.saveAsTable(dashboardName)
	}

}
