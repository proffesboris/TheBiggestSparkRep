package ru.sberbank.sdcb.k7m.core.pack.STG.EXEC

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_exec(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_exec_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_exec_ShortName)


	val dataframe = spark.sql(
		s"""
			|
			|
			|SELECT
			|    ex2.ul_org_id
			|    , ex2.e_ul_inn
			|    , ex2.e_fio -- ФИО
			|    , ex2.e_birth_dt -- Год Рождения
			|    , cast(ex2.e_share_prcnt_cnt as decimal(22,5)) / 100 as e_share_prcnt_cnt
			|    , ex2.e_post_nm
			|FROM
			|    (
			|        SELECT ex1.ul_org_id,
			|                ex1.e_birth_dt,
			|                ex1.e_fio,
			|                ex1.e_share_prcnt_cnt,
			|                ex1.e_post_nm ,
			|                ex1.e_ul_inn
			|        FROM
			|            (
			|            SELECT
			|                        ex.*,
			|                        row_number() over (
			|                                partition by ex.e_ul_inn, ex.e_fio, ex.e_birth_dt order by ex.effectivefrom desc, ex.e_slice_id desc) as rn
			|            FROM $ZeroLayerSchema.int_executive ex
			|            WHERE '$DATE' >= ex.effectivefrom
			|            ) ex1
			|    WHERE ex1.rn=1
			|    ) ex2
			|WHERE
			|(ex2.e_ul_inn is not null )
			|    AND (ex2.e_fio is not null )
			|    AND (ex2.e_fio <> '' )
			|    AND (ex2.e_birth_dt is not null)
			|    AND (ex2.e_birth_dt <> '')
			|
			|
		""".stripMargin)


}
