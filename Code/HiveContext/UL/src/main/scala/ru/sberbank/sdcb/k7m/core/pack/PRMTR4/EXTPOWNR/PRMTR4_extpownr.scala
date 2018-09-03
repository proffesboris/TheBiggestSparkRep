package ru.sberbank.sdcb.k7m.core.pack.PRMTR4.EXTPOWNR

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR4_extpownr(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR4_extpownr_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR4_extpownr_ShortName)

	val dds_rel_Name: String = genDashBoardName(DDS_rel_ShortName)
	val dds_exec_Name: String = genDashBoardName(DDS_exec_ShortName)
	val prmtr3_ownr_fl_Name: String = genDashBoardName(PRMTR3_ownr_fl_ShortName)

	val PRMRT4_EXTPOWNR_T1 = spark.sql(
		s"""
			 |SELECT
			 |    inn as inn1 -- ИНН
			 |  --  , fio as nm  -- ФИО (Добавил Семен)
			 |    , inn_established as inn2
			 |    , 0 as quantity
			 |    , 0 as max_share_flag
			 |    , 0 as is_ownr
			 |    FROM $dds_exec_Name
			 |UNION ALL
			 |    SELECT inn1
			 |          --  , nm -- ФИО (Добавил Семен)
			 |            , inn2
			 |            , share_sum as quantity
			 |            , max_share_flag
			 |            , 1 as is_ownr
			 |    FROM $prmtr3_ownr_fl_Name
		""".stripMargin)

	PRMRT4_EXTPOWNR_T1.createOrReplaceTempView("PRMRT4_EXTPOWNR_T1")

	val dataframe = spark.sql(
		s"""
			|SELECT DISTINCT tt.inn1 -- ИНН
			|            --    , tt.nm -- ФИО (Добавил Семен)
			|                , tt.inn2
			|                , tt.quantity
			|                , tt.max_share_flag
			|                , tt.is_ownr
			|                , tt.family_id
			|FROM
			|    (
			|    SELECT
			|            t.inn1 -- ИНН
			|        --    , t.nm -- ФИО (Добавил Семен)
			|            , t.inn2
			|            , t.quantity
			|            , t.max_share_flag
			|            , t.is_ownr
			|            , r.family_id
			|            FROM PRMRT4_EXTPOWNR_T1 t
			|            LEFT JOIN $dds_rel_Name r
			|        ON (r.inn=t.inn1)
			|    ) tt
		""".stripMargin)

}
