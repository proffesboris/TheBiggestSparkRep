package ru.sberbank.sdcb.k7m.core.pack.STG.FND_RS

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_fnd_rs_T2(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_fnd_rs_T2_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_fnd_rs_T2_ShortName)

	private val Stg_fnd_rs_T1_name = genDashBoardName(STG_fnd_rs_T1_ShortName)

	val dataframe = spark.sql(
		s"""
			|
			|SELECT a.*
			|FROM $Stg_fnd_rs_T1_name a
			|LEFT JOIN
			|    (
			|        SELECT ul_org_id_established, max(effectivefrom) as max_effectivefrom
			|        FROM $Stg_fnd_rs_T1_name
			|        group by ul_org_id_established
			|        having (sum(of_share_prcnt_cnt) - 1) > 0.000001
			|    ) b
			|ON (a.ul_org_id_established=b.ul_org_id_established)
			|WHERE
			|    b.ul_org_id_established is null
			|    OR
			|    (
			|        abs(a.of_share_prcnt_cnt-1) < 0.000001 AND a.effectivefrom=b.max_effectivefrom
			|    )
			|
			|
		""".stripMargin)

}
