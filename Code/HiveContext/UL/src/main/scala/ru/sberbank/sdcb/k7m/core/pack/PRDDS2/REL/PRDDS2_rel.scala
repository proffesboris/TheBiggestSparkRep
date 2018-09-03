package ru.sberbank.sdcb.k7m.core.pack.PRDDS2.REL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRDDS2_rel(config: Config) extends Table(config: Config){


	val dashboardName: String = genDashBoardName(PRDDS2_rel_ShortName)
	val dashboardPath: String = genDashBoardPath(PRDDS2_rel_ShortName)


	val dataframe = spark.table(genDashBoardName(STG_appl_ShortName)) // ПОКА ПРОСТО ЗАГЛУШКА

	val STG_appl = spark.table(genDashBoardName(STG_appl_ShortName))
	val STG_rel  = spark.table(genDashBoardName(STG_rel_ShortName))
	val STG_ip 	 = spark.table(genDashBoardName(STG_ip_ShortName))

	val STG_appl_broadcasted = spark.sparkContext.broadcast(STG_appl)
	val STG_rel_broadcasted  = spark.sparkContext.broadcast(STG_rel)
	val STG_ip_broadcasted   = spark.sparkContext.broadcast(STG_ip)

	STG_appl_broadcasted.value.createOrReplaceTempView("STG_appl")
	STG_rel_broadcasted.value.createOrReplaceTempView("STG_rel")
	STG_ip_broadcasted.value.createOrReplaceTempView("STG_ip")

	var n = 2

	def create(): Unit = {

		val STG_rel_union_Name = genDashBoardName(STG_rel_union_ShortName)
		val STG_rel_union_Path = genDashBoardPath(STG_rel_union_ShortName)

		val stg_rel_union = spark.sql(
			s"""
				 |
				 |	SELECT cast(null as string) as inn
				 |			, CASE WHEN fio$n='' THEN cast(null as string) ELSE fio$n END as fio
				 |			, CASE WHEN birth_dt$n='' THEN cast(null as string) ELSE birth_dt$n END as birth_dt
				 |			, sys_recordkey FROM stg_rel
				 |
			|
		""".stripMargin)

		save(stg_rel_union, STG_rel_union_Name, STG_rel_union_Path)

		for (k <- 0 until 12) {
			n = k + 3
			val sql_query = spark.sql(
				s"""
					SELECT cast(null as string) as inn
							, CASE WHEN fio$n='' THEN cast(null as string) ELSE fio$n END as fio
							, CASE WHEN birth_dt$n='' THEN cast(null as string) ELSE birth_dt$n END as birth_dt
							, sys_recordkey FROM stg_rel
			""")
			sql_query.repartition(450).write.mode("append").option("path", dashboardPath).format("parquet").saveAsTable(STG_rel_union_Name)
		}

		val STG_rel_union = spark.sql(
			s"""
				 |
				 |select distinct inn, fio, birth_dt, sys_recordkey
				 |                      from $STG_rel_union_Name
				 |                      where (inn is not null) or ((fio is not null) and (birth_dt is not null))
				 |
				 |
		""".stripMargin)

		STG_rel_union.createOrReplaceTempView("stg_rel_union")

		val PRDDS2_rel_T1_part1 = spark.sql(
			"""
			|
			|SELECT coalesce(r.inn, app.inn, ip.inn) as inn, lower(r.fio) as fio, r.birth_dt, r.sys_recordkey, ip.ip_flag, 0 as was_appl
			|FROM  stg_rel_union r
			|
			|LEFT JOIN stg_appl app
			|    ON (lower(r.fio)=lower(app.fio) AND r.birth_dt=app.birth_dt)
			|LEFT JOIN stg_ip ip
			|    ON (lower(ip.fio)=lower(r.fio) and ip.birth_dt=r.birth_dt)
			|WHERE coalesce(r.inn, app.inn, ip.inn) is not null OR (r.fio is not null and r.birth_dt is not null)
			|
		""".stripMargin)

		val PRDDS2_rel_T1_Name = SaveSchema + "." + prefix + "prdds2_rel_T1"
		val PRDDS2_rel_T1_Path = config.paPath + prefix + "prdds2_rel_T1"

		save(PRDDS2_rel_T1_part1, PRDDS2_rel_T1_Name, PRDDS2_rel_T1_Path)

		val PRDDS2_rel_T1_part2 = spark.sql(
			"""
				| SELECT a.inn, a.fio, a.birth_dt, a.sys_recordkey, ip.ip_flag, 1 as was_appl
				|     FROM stg_appl a
				|    LEFT JOIN stg_ip ip
				|    ON (a.inn=ip.inn)
				|    WHERE a.inn is not null OR (a.fio is not null and a.birth_dt is not null)
			""".stripMargin)

		PRDDS2_rel_T1_part2.write.mode("append").option("path", dashboardPath).format("parquet").saveAsTable(PRDDS2_rel_T1_Name)


		val PRDDS2_rel_T1 = spark.table(PRDDS2_rel_T1_Name)


		PRDDS2_rel_T1.createOrReplaceTempView("prdds2_rel_T1")

		val pRDDS2_rel = spark.sql(
			"""
				|
				|SELECT *,
				|    count(*) over (partition by lower(fio), birth_dt) as cnt_fbt,
				|    count(*) over (partition by inn) as cnt_inn
				|FROM (SELECT DISTINCT fio, birth_dt, inn, ip_flag, sys_recordkey, was_appl FROM prdds2_rel_T1
				|    where (inn is not null) and inn <> '111111111111'
				|) t
				|
				|
			""".stripMargin)

		save(pRDDS2_rel, dashboardName, dashboardPath)


	}

}
