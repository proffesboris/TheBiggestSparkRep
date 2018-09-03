package ru.sberbank.sdcb.k7m.core.pack.PRMTR1.BEN_CRM

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR1_ben_crm(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR1_ben_crm_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR1_ben_crm_ShortName)

	val CRM_BENFR_TABLE: String = genDashBoardName(CRM_BENFR_TABLE_ShortName)

	val bencrm = spark.sql(
		s"""
			|select
			|         inn_enrich as inn1
			|        , inn as inn2
			|        , fz_ben as id1 -- потом поменять на целевой id
			|        , crm_id as crm_id2
			|        , fst_name
			|        , mid_name
			|        , last_name
			|        , birth_dt
			|        , trim(lower(concat(coalesce(concat(last_name,' '), ''),
			|                            coalesce(concat(fst_name,' '), ''),
			|                            coalesce(mid_name, '')))) as fio
			|from $CRM_BENFR_TABLE
			|where (inn not like '0000000%')
			|    and (inn <> '')
			|    and (inn is not null)
			|    and (fz_ben is not null)
		""".stripMargin)

	bencrm.createOrReplaceTempView("bencrm")

	val dataframe = spark.sql(
		s"""
			|select a.*
			|from bencrm a
			|
			|inner join
			|(
			|    select inn1
			|    from bencrm
			|    group by inn1
			|    having count(distinct id1) <= $CRM_CLEAR_COUNT
			|) b
			|on a.inn1 = b.inn1
			|
			|inner join
			|(
			|    select id1
			|    from bencrm
			|    group by id1
			|    having count(distinct inn1) <= $CRM_CLEAR_COUNT
			|) c
			|on a.id1 = c.id1
		""".stripMargin)

}
