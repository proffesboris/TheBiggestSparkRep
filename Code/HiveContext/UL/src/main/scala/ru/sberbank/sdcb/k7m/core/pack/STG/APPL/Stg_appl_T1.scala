package ru.sberbank.sdcb.k7m.core.pack.STG.APPL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_appl_T1(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_appl_T1_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_appl_T1_ShortName)

	private val Stg_appl_T0_restored_name = genDashBoardName(STG_appl_T0_restored_ShortName)

	val dataframe = spark.sql(
		s"""
    |select a.*
    |from $Stg_appl_T0_restored_name a
    |left join
    |(
    |select inn
    |    from $Stg_appl_T0_restored_name
    |    group by inn
    |    having count(distinct fio) > 1
    |) b
    |on (a.inn = b.inn)
    |where b.inn is null
		""".stripMargin)

}
