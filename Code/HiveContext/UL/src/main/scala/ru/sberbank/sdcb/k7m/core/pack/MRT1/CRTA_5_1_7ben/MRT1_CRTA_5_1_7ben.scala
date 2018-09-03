package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_7ben

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_7ben(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_7ben_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_7ben_ShortName)

	val prmtr1_ben_crm_Name: String = genDashBoardName(PRMTR1_ben_crm_ShortName)
	val prmtr4_bencrm_ex_Name: String = genDashBoardName(PRMTR4_bencrm_ex_ShortName)

	val dataframe = spark.sql(
		s"""
			|select
			|             t.inn1
			|       --      , t.id1 -- BEN ID (Добавил Семен)
			|            , t.inn2
			|            , '5.1.7ben' as criterion
			|            , 100 as confidence
			|            , cast(1 as decimal(22, 5)) as quantity
			|            , '$DATE' as dt
			|from
			|(
			|    select distinct a.inn2 as inn1, b.inn2 as inn2 --, a.id1 as id1 -- BEN ID (Добавил Семен)
			|    from $prmtr1_ben_crm_Name a
			|    inner join $prmtr1_ben_crm_Name b
			|    on ((a.id1 = b.id1) and (a.inn2 <> b.inn2))
			|    	or ((a.inn1 = b.inn1) and (a.inn2 <> b.inn2))
			|) t
		""".stripMargin)

	val bengd = spark.sql(
		s"""
			|SELECT
			|        t.inn1
			|       -- , max(t.id1) as id1 -- BEN ID (Добавил Семен)
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
			|           -- , p1.id1 -- BEN ID (Добавил Семен)
			|            , p2.inn2
			|            , '5.1.7bengd' as criterion
			|            , p1.quantity
			|            ,p2.quantity as quantity2
			|            ,p1.inn1 as fl1
			|    FROM $prmtr4_bencrm_ex_Name p1
			|    INNER JOIN $prmtr4_bencrm_ex_Name p2
			|    ON (p1.inn1=p2.inn1 and ( (p1.is_ben=1 and p2.is_ben=0) or (p1.is_ben=0 and p2.is_ben=1) ) and (p1.inn2 <> p2.inn2))
			|
			|) t
			|GROUP BY t.inn1, t.inn2, t.criterion
		""".stripMargin)

	val benrel = spark.sql(
		s"""
			|select distinct aa.*
			|from
			|(
			|SELECT
			|            t.inn1
			|        --    , max(t.id1) as id1 -- BEN ID (Добавил Семен)
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
			|        --    , p1.id1 -- BEN ID (Добавил Семен)
			|            , p2.inn2
			|            , '5.1.7benrel' as criterion
			|            , p1.quantity
			|            , p2.quantity as quantity2
			|            , p1.family_id as fid
			|            , p1.inn1 as fl1
			|            , p2.inn2 as fl2
			|
			|
			|    FROM
			|    (
			|        select inn1, inn2, family_id, max(quantity) as quantity --, max(id1) as id1 -- BEN ID (Добавил Семен)
			|        from $prmtr4_bencrm_ex_Name
			|        group by inn1, inn2, family_id
			|    ) p1
			|    INNER JOIN
			|    (
			|        select inn1, inn2, family_id, max(quantity) as quantity --, max(id1) as id1 -- BEN ID (Добавил Семен)
			|        from $prmtr4_bencrm_ex_Name
			|        group by inn1, inn2, family_id
			|    ) p2
			|    ON ((p1.family_id = p2.family_id) and (p1.inn2 <> p2.inn2) and (p1.inn1 <> p2.inn1))
			|) t
			|GROUP BY t.inn1, t.inn2, t.criterion
			|) aa
			|left join
			|$prmtr4_bencrm_ex_Name bb
			|on aa.inn1 = bb.inn2
			|
			|left join
			|$prmtr4_bencrm_ex_Name cc
			|on aa.inn2 = cc.inn2
			|
			|where (bb.is_ben = 1 or cc.is_ben = 1)
		""".stripMargin)

	def create(): Unit = {
		save()
		append(bengd, dashboardName, dashboardPath)
		append(benrel, dashboardName, dashboardPath)
	}

}
