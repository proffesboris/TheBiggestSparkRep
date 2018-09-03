package ru.sberbank.sdcb.k7m.core.pack.STG.FND_RS

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_fnd_rs(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_fnd_rs_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_fnd_rs_ShortName)

	val Stg_fnd_rs_T2_name: String = genDashBoardName(STG_fnd_rs_T2_ShortName)

	val dataframe = spark.sql(
		s"""
			|select * from $Stg_fnd_rs_T2_name
			|where ul_org_id_established not in
			|
			|(
			|    select ul_org_id_established
			|
			|    from $Stg_fnd_rs_T2_name
			|
			|    group by ul_org_id_established
			|    having (sum(of_share_prcnt_cnt) - 1) > 0.00001
			|)
		""".stripMargin)

}
