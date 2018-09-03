package ru.sberbank.sdcb.k7m.core.pack.DDS.REL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class DDS_rel_TX2(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(DDS_rel_TX2_ShortName)
	val dashboardPath: String = genDashBoardPath(DDS_rel_TX2_ShortName)

	val DDS_rel_TX_Name: String = genDashBoardName(DDS_rel_TX_ShortName)

	spark.sql(s"""
SELECT *, min(family_id) over (partition by fio, inn) as  sys_recordkey_1
FROM $DDS_rel_TX_Name
where inn is not null
""").createOrReplaceTempView("dds_rel_T11")

	spark.sql("""
SELECT *, min(sys_recordkey_1) over (partition by family_id) as sys_recordkey_2
FROM dds_rel_T11
""").createOrReplaceTempView("dds_rel_T12")

	val dataframe = spark.sql("""
SELECT inn, ip_flag, fio, sys_recordkey_2 as family_id , max(birth_dt) as birth_dt
FROM dds_rel_T12
group by inn, ip_flag, fio, sys_recordkey_2
""")

}
