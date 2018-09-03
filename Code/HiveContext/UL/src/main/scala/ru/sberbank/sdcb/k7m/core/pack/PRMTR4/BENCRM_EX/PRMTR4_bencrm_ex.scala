package ru.sberbank.sdcb.k7m.core.pack.PRMTR4.BENCRM_EX

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR4_bencrm_ex(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR4_bencrm_ex_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR4_bencrm_ex_ShortName)

	val dds_rel_Name: String = genDashBoardName(DDS_rel_ShortName)
	val dds_exec_Name: String = genDashBoardName(DDS_exec_ShortName)
	val prmtr1_ben_crm_cruel = genDashBoardName(PRMTR1_ben_crm_cruel_ShortName)

	val PRMRT4_BENCRM_EX_T1 = spark.sql(
		s"""
			 |SELECT
			 |    inn as inn1
			 |    , NULL as id1 -- BEN ID (Добавил Семен)
			 |    , inn_established as inn2
			 |    , 0 as is_ben
			 |    , cast(0 as decimal(22, 5)) as quantity
			 |    FROM $dds_exec_Name
			 |UNION ALL
			 |    SELECT inn1
			 |    				, id1 -- BEN ID (Добавил Семен)
			 |            , inn2
			 |            , 1 as is_ben
			 |            , cast(1 as decimal(22, 5)) as quantity
			 |    FROM $prmtr1_ben_crm_cruel
		""".stripMargin)

	PRMRT4_BENCRM_EX_T1.createOrReplaceTempView("PRMRT4_BENCRM_EX_T1")

	val dataframe = spark.sql(
		s"""
			 |SELECT DISTINCT tt.inn1
			 |                , tt.id1 -- BEN ID (Добавил Семен)
			 |                , tt.inn2
			 |                , tt.is_ben
			 |                ,tt.quantity
			 |                , tt.family_id
			 |FROM
			 |    (
			 |    SELECT
			 |            t.inn1
			 |            , t.id1 -- BEN ID (Добавил Семен)
			 |            , t.inn2
			 |            , t.is_ben
			 |            , t.quantity
			 |            , r.family_id
			 |            FROM PRMRT4_BENCRM_EX_T1 t
			 |            LEFT JOIN $dds_rel_Name r
			 |        ON (r.inn=t.inn1)
			 |    ) tt
		""".stripMargin)

}
