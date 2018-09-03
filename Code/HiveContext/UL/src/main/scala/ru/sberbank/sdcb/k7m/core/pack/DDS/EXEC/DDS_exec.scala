package ru.sberbank.sdcb.k7m.core.pack.DDS.EXEC

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class DDS_exec(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(DDS_exec_ShortName)
	val dashboardPath: String = genDashBoardPath(DDS_exec_ShortName)

	private val Stg_UL_Name  = genDashBoardName(STG_UL_ShortName)
	private val Stg_tr_prsn_Name = genDashBoardName(STG_tr_prsn_ShortName)

	val dataframe = spark.sql(
		s"""
			|
			|select *
			|from
			|(
			|SELECT DISTINCT
			|    u.inn as inn_established,
			|    u.eg_ul_head_nm as post,
			|    coalesce(u.eg_ul_head_inn, tp.tp_inn) as inn, --ИНН
			|    coalesce(u.head_fio, tp.fio) as fio -- ФИО
			|FROM
			|(
			|        select *
			|        from $Stg_UL_Name
			|        where gendir=1
			|) u
			|LEFT JOIN
			|(
			|    select *
			|    from $Stg_tr_prsn_Name
			|    where gendir=1
			|) tp
			|ON (u.egrul_org_id=tp.egrul_org_id)
			|WHERE coalesce(u.eg_ul_head_inn, tp.tp_inn) is not null
			|) where (inn <> '') and (inn not like '00000000%') and ((length(inn) = 10) or length(inn) = 12) and (inn is not null)
			|
			|
			|
		""".stripMargin)

}
