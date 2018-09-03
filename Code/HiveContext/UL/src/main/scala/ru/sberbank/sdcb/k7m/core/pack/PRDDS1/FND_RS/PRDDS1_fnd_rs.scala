package ru.sberbank.sdcb.k7m.core.pack.PRDDS1.FND_RS

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRDDS1_fnd_rs(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRDDS1_fnd_rs_ShortName)
	val dashboardPath: String = genDashBoardPath(PRDDS1_fnd_rs_ShortName)

	private val Stg_fnd_rs_name = genDashBoardName(STG_fnd_rs_ShortName)
	private val Stg_ul_name 	  = genDashBoardName(STG_UL_ShortName)

	val dataframe = spark.sql(
		s"""
			|
			|
			|SELECT
			|        ofrs.ul_org_id_founder,
			|        u.egrul_org_id as egrul_org_id_established,
			|        of_share_prcnt_cnt,
			|        ofrs.of_type,
			|        ofrs.effectivefrom,
			|        cast(null as string) as of_inn ,
			|        ofrs.of_nm, -- ФИО
			|        u.inn
			|        , rank() over (partition by u.egrul_org_id order by ofrs.effectivefrom desc) as rn
			|FROM $Stg_fnd_rs_name ofrs
			|INNER JOIN $Stg_ul_name u
			|        ON (ofrs.ul_org_id_established=u.ul_org_id)
			|where of_share_prcnt_cnt > 0.00000001
			|
		""".stripMargin)

}
