package ru.sberbank.sdcb.k7m.core.pack.STG.APPL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Table}

class Stg_appl_T2(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_appl_T2_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_appl_T2_ShortName)

	private val Stg_appl_T1_name = genDashBoardName(STG_appl_T1_ShortName)

	val dataframe = spark.sql(
		s"""
    |select a.*
    |from $Stg_appl_T1_name a
    |left join
    |(
    |select
    |        t_629_a1_pass_num
    |         , t_628_a1_pass_seria
    |        from $Stg_appl_T1_name
    |        where nvl(t_629_a1_pass_num, '') <> '' and nvl(t_628_a1_pass_seria, '') <> ''
    |        group by t_629_a1_pass_num, t_628_a1_pass_seria
    |        having count (distinct inn) > 1
    |) b
    |on (a.t_629_a1_pass_num = b.t_629_a1_pass_num) and (a.t_628_a1_pass_seria = b.t_628_a1_pass_seria)
    |where b.t_628_a1_pass_seria is null
		""".stripMargin)

}
