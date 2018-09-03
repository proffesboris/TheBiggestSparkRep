package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_9

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_9(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_9_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_9_ShortName)

	val prmrt4_bg_ownr_Name = genDashBoardName(PRMTR4_bg_ownr_ShortName)
	val prmrt4_bh_ownr_Name = genDashBoardName(PRMTR4_bh_ownr_ShortName)

	val MRT1_Crta_5_1_9_T1 = spark.sql(
		s"""
			|SELECT DISTINCT t.inn1, -- t.nm, -- ФИО (Добавил Семен)
			|t.inn2, t.quantity
			|FROM
			|    (
			|        SELECT p1.inn2 as inn1, p2.inn2, 0 as quantity --, p1.nm -- ФИО (Добавил Семен)
			|        FROM $prmrt4_bg_ownr_Name p1
			|        INNER JOIN $prmrt4_bg_ownr_Name p2
			|        ON (p1.inn1=p2.inn1 and p1.inn2<>p2.inn2)
			|    UNION ALL
			|        SELECT p1.inn1, p2.inn1 as inn2, 1 as quantity --, p1.nm -- ФИО (Добавил Семен)
			|        FROM $prmrt4_bh_ownr_Name p1
			|        INNER JOIN $prmrt4_bh_ownr_Name p2
			|        ON (p1.inn2=p2.inn2 AND p1.inn1<>p2.inn1)
			|    ) t
		""".stripMargin)


	MRT1_Crta_5_1_9_T1.createOrReplaceTempView("MRT1_crta_5_1_9_T1")

	val dataframe = spark.sql(
		s"""
			|SELECT distinct
			|                inn1
			|              --  nm -- ФИО (Добавил Семен)
			|                , inn2
			|                , '5.1.9' as criterion
			|                , 100 as confidence
			|                , CASE WHEN cnt=2 THEN 2 ELSE quantity END as quantity
			|                , '$DATE' as dt
			|from
			|(
			|    SELECT
			|            inn1
			|           -- nm -- ФИО (Добавил Семен)
			|            , inn2
			|            , quantity
			|            , count(*) over (partition by inn1, inn2) as cnt
			|    FROM MRT1_crta_5_1_9_T1
			|)
			|
		""".stripMargin)

}
