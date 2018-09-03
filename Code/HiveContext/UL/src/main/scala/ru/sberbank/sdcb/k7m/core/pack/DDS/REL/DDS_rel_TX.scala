package ru.sberbank.sdcb.k7m.core.pack.DDS.REL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class DDS_rel_TX(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(DDS_rel_TX_ShortName)
	val dashboardPath: String = genDashBoardPath(DDS_rel_TX_ShortName)

	val PRDDS2_rel_Name: String = genDashBoardName(PRDDS2_rel_ShortName)

	val DDS_rel_T1 = spark.sql(
		s"""
			|
			|SELECT DISTINCT
			|            fio -- ФИО
			|            , birth_dt -- Дата Рождения
			|            , sys_recordkey
			|            , CASE WHEN (cnt_fbt>1 and was_appl=0)  OR (cnt_inn>1 and was_appl=0) THEN null ELSE inn END as inn -- ИНН
			|            , CASE WHEN (cnt_fbt>1 and was_appl=0)  OR (cnt_inn>1 and was_appl=0) THEN 0 ELSE ip_flag END as ip_flag
			|FROM $PRDDS2_rel_Name
			|WHERE (cnt_fbt < $NUMBER_APPL) and (lower(fio)<>'нет данных нет данных нет данных')
			|
			|
		""".stripMargin)

			DDS_rel_T1.createOrReplaceTempView("dds_rel_T1")

			spark.sql("""
		SELECT *, min(sys_recordkey) over (partition by fio, birth_dt, inn) as  sys_recordkey_1
		FROM dds_rel_T1
		where inn is not null
		""").createOrReplaceTempView("dds_rel_T2")

			spark.sql("""
		SELECT *, min(sys_recordkey_1) over (partition by sys_recordkey) as sys_recordkey_2
		FROM dds_rel_T2
		""").createOrReplaceTempView("dds_rel_T3")

			spark.sql("""
		SELECT *, min(sys_recordkey_2) over (partition by sys_recordkey_1) as sys_recordkey_3
		FROM dds_rel_T3
		""").createOrReplaceTempView("dds_rel_T4")

			spark.sql("""
		SELECT *, min(sys_recordkey_3) over (partition by sys_recordkey_2) as sys_recordkey_4
		FROM dds_rel_T4
		""").createOrReplaceTempView("dds_rel_T5")

			spark.sql("""
		SELECT *, min(sys_recordkey_4) over (partition by sys_recordkey_3) as sys_recordkey_5
		FROM dds_rel_T5
		""").createOrReplaceTempView("dds_rel_T6")

			spark.sql("""
		SELECT *, min(sys_recordkey_5) over (partition by sys_recordkey_4) as sys_recordkey_6
		FROM dds_rel_T6
		""").createOrReplaceTempView("dds_rel_T7")

			spark.sql("""
		SELECT *, min(sys_recordkey_6) over (partition by sys_recordkey_5) as sys_recordkey_7
		FROM dds_rel_T7
		""").createOrReplaceTempView("dds_rel_T8")

			spark.sql("""
		SELECT DISTINCT inn, ip_flag, fio, birth_dt, sys_recordkey_7 as sys_recordkey_8
		FROM dds_rel_T8
		""").createOrReplaceTempView("dds_rel_T9")

			spark.sql("""
		SELECT DISTINCT inn, ip_flag, fio, birth_dt, sys_recordkey_8 as sys_recordkey_9
		FROM dds_rel_T9
		""").createOrReplaceTempView("dds_rel_T10")

	val dataframe =	spark.sql("""
		SELECT DISTINCT inn, ip_flag, fio, birth_dt, sys_recordkey_9 as family_id
		FROM dds_rel_T10
		""")



}
