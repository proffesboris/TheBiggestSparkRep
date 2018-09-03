package ru.sberbank.sdcb.k7m.core.pack.STG.APPL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_appl(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_appl_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_appl_ShortName)

	private val Stg_appl_T2_name = genDashBoardName(STG_appl_T2_ShortName)

	val dataframe = spark.sql(
		s"""
    |select distinct a.inn, a.fio, a.sys_recordkey, b.birth_dt
    |from $Stg_appl_T2_name a
    |left join
    |(
    |select
    |        inn, fio, max(birth_dt) as birth_dt
    |        from $Stg_appl_T2_name
    |        group by inn, fio
    |) b
    |on (a.inn = b.inn) and (a.fio = b.fio)
		""".stripMargin)

}
