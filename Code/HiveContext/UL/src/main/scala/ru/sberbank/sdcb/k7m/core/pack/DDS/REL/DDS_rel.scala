package ru.sberbank.sdcb.k7m.core.pack.DDS.REL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class DDS_rel(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(DDS_rel_ShortName)
	val dashboardPath: String = genDashBoardPath(DDS_rel_ShortName)

	val DDS_rel_TX2_Name: String = genDashBoardName(DDS_rel_TX2_ShortName)

	val dataframe = spark.sql(
		s"""
			|
			|select a.*
			|from
			|$DDS_rel_TX2_Name a
			|left join
			|    (
			|        select family_id, count(*) as cnt
			|        from $DDS_rel_TX2_Name
			|        group by family_id
			|        having count(*) > $FAMILY_SIZE
			|    ) b
			|on a.family_id = b.family_id
			|where b.family_id is null
			|
		""".stripMargin)

}
