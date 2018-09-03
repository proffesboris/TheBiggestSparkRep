package ru.sberbank.sdcb.k7m.core.pack.STG.DIR_BRD

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_dir_brd(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_dir_brd_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_dir_brd_ShortName)

	val dataframe = spark.sql(
		s"""
			|
			|SELECT
			|        bd2.ul_org_id
			|        , bd2.bd_fio -- ФИО
			|        , bd2.bd_birth_dt -- Год Рождения
			|        , bd2.bd_post_nm
			|        , bd2.bd_ul_inn
			|FROM
			|    (
			|    SELECT
			|            bd1.ul_org_id
			|            , bd1.bd_birth_dt
			|            , bd1.bd_fio
			|            , bd1.bd_post_nm
			|            , bd1.bd_ul_inn
			|    FROM
			|        (
			|            SELECT
			|                    bd.*,
			|                    row_number() over (partition by
			|                        bd.bd_ul_inn, bd.bd_fio, bd.bd_birth_dt order by bd.effectivefrom desc, bd.bd_slice_id desc) as rn
			|        FROM $ZeroLayerSchema.int_board_of_directors bd
			|        WHERE '$DATE' >= bd.effectivefrom) bd1
			|    WHERE bd1.rn=1
			|    ) bd2
			|WHERE nvl(bd2.bd_ul_inn,'') <> ''
			|    AND (bd2.bd_fio is not null)
			|    AND (bd2.bd_fio <> '')
			|    AND (bd2.bd_birth_dt is not null)
			|    AND (bd2.bd_birth_dt <> '')
			|
		""".stripMargin)

}
