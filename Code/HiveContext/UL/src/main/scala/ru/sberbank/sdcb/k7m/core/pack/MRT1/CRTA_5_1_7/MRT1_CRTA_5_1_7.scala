package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_7

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_7(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_7_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_7_ShortName)

	val prmtr3_ownr_fl_Name = genDashBoardName(PRMTR3_ownr_fl_ShortName)

	val MRT1_CRTA_5_1_7_T0 = spark.table(genDashBoardName(MRT1_Crta_5_1_7_T0_ShortName))

	MRT1_CRTA_5_1_7_T0.createOrReplaceTempView("MRT1_crtra_5_1_7_T0")

	val crta_5_1_7ow = spark.sql(
		s"""
			|SELECT
			|        t.inn1 -- ИНН
			|     --   , max(t.nm) as  nm -- ФИО (Добавил Семен)
			|        , t.inn2
			|        , t.criterion
			|        , 100 as confidence
			|       -- , quantity
			|      --  , quantity2
			|        , sum(t.quantity) as quantity
			|   --      ,fl1
			|
			|
			|
			|        , '$DATE' as dt
			|FROM
			|    (
			|    SELECT
			|    distinct
			|            p1.inn2 as inn1
			|        --    , p1.nm -- ФИО (Добавил Семен)
			|            , p2.inn2
			|            , '5.1.7ow' as criterion
			|            , p1.quantity
			|            ,p2.quantity as quantity2
			|            ,p1.inn1 as fl1
			|
			|    FROM
			|             MRT1_crtra_5_1_7_T0 p1
			|
			|    INNER JOIN
			|
			|             MRT1_crtra_5_1_7_T0 p2
			|
			|    ON ((p1.inn1 = p2.inn1) AND (p1.inn2 <> p2.inn2) and (p1.is_ownr = 1) and (p2.is_ownr = 1))
			|
			|) t
			|GROUP BY t.inn1, t.inn2, t.criterion
		""".stripMargin)

	val crta_5_1_7_owgd = spark.sql(
		s"""
			|SELECT
			|        t.inn1
			|     --   , max(t.nm) as nm -- ФИО (Добавил Семен)
			|        , t.inn2
			|        , t.criterion
			|        , 100 as confidence
			|        , sum(t.quantity) as quantity
			|        , '$DATE' as dt
			|FROM
			|    (
			|    SELECT
			|     distinct
			|            p1.inn2 as inn1
			|       --     , p1.nm -- ФИО (Добавил Семен)
			|            , p2.inn2
			|            , '5.1.7owgd' as criterion
			|            , p1.quantity
			|            ,p2.quantity as quantity2
			|            ,p1.inn1 as fl1
			|    FROM MRT1_crtra_5_1_7_T0 p1
			|    INNER JOIN MRT1_crtra_5_1_7_T0 p2
			|    ON (p1.inn1=p2.inn1 and ( (p1.is_ownr=1 and p2.is_ownr=0) or (p1.is_ownr=0 and p2.is_ownr=1) ) and (p1.inn2 <> p2.inn2))
			|
			|) t
			|GROUP BY t.inn1, t.inn2, t.criterion
		""".stripMargin)

	val dataframe = spark.sql(
		s"""
			|SELECT
			|        inn1 -- ИНН
			|    --    ,nm -- ФИО (Добавил Семен)
			|        , inn2
			|        , '5.1.7' as criterion
			|        , 100 as confidence
			|        , share_sum as quantity
			|        , '$DATE' as dt
			|        FROM $prmtr3_ownr_fl_Name
			|        WHERE share_sum >= 0.5
		""".stripMargin)

	val dataframe_appended = spark.sql(
		s"""
			|SELECT
			|            t.inn1
			|      --      , max(t.nm) as nm -- ФИО (Добавил Семен)
			|            , t.inn2
			|            , t.criterion
			|            , 50 as confidence
			|            , sum(t.quantity) as quantity
			|            , '$DATE' as dt
			|FROM
			|    (
			|    SELECT
			|         distinct
			|            p1.inn2 as inn1
			|      --    ,  p1.nm -- ФИО (Добавил Семен)
			|            , p2.inn2
			|            , '5.1.7rel' as criterion
			|            , p1.quantity
			|            , p2.quantity as quantity2
			|            , p1.family_id as fid
			|            , p1.inn1 as fl1
			|            , p2.inn2 as fl2
			|
			|
			|    FROM
			|    (
			|        select inn1, -- max(nm) as nm, -- ФИО (Добавил Семен)
			|        inn2, family_id, max(quantity) as quantity
			|        from MRT1_crtra_5_1_7_T0
			|        group by inn1, inn2, family_id
			|    ) p1
			|    INNER JOIN
			|    (
			|        select inn1, -- max(nm) as nm, -- ФИО (Добавил Семен)
			|        inn2, family_id, max(quantity) as quantity
			|        from MRT1_crtra_5_1_7_T0
			|        group by inn1, inn2, family_id
			|    ) p2
			|    ON ((p1.family_id = p2.family_id) and (p1.inn2 <> p2.inn2) and (p1.inn1 <> p2.inn1) )
			|) t
			|GROUP BY t.inn1, t.inn2, t.criterion
		""".stripMargin)

	def create(): Unit = {
		save(crta_5_1_7ow, dashboardName, dashboardPath)
		append(crta_5_1_7_owgd, dashboardName, dashboardPath)
		append(dataframe, dashboardName, dashboardPath)
		append(dataframe_appended, dashboardName, dashboardPath)
	}

}
