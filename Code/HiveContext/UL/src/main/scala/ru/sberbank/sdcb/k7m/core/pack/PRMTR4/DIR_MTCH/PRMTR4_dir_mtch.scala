package ru.sberbank.sdcb.k7m.core.pack.PRMTR4.DIR_MTCH

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR4_dir_mtch(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR4_dir_mtch_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR4_dir_mtch_ShortName)

	val prmrt1_dir_aggr: String = genDashBoardName(PRMTR1_dir_aggr_ShortName)

	val dataframe = spark.sql(
		s"""
			 |SELECT DISTINCT
			 |            cn.inn1
			 |            , cn.inn2
			 |            , greatest(cn.cnt_jnd/cn.cnt1, cn.cnt_jnd/cn.cnt2) as max_share_dir_brd
			 |FROM
			 |    (
			 |            SELECT
			 |                    jnd.inn1
			 |                    , jnd.inn2
			 |                    , jnd.cnt1
			 |                    , jnd.cnt2
			 |                    , count(*) over (partition by jnd.inn1, jnd.inn2) as cnt_jnd
			 |    FROM
			 |        (
			 |                SELECT
			 |                        da1.inn_established as inn1
			 |                        , da2.inn_established as inn2
			 |                        , da1.cnt_dir as cnt1
			 |                        , da2.cnt_dir as cnt2
			 |        FROM $prmrt1_dir_aggr da1
			 |        INNER JOIN $prmrt1_dir_aggr da2
			 |        ON (lower(da1.fio)=lower(da2.fio) AND da1.birth_year=da2.birth_year and da1.inn_established <> da2.inn_established)
			 |        ) jnd
			 |    ) cn
			 |WHERE ((cn.cnt_jnd/cn.cnt1>=0.5) or (cn.cnt_jnd/cn.cnt2>=0.5))
		""".stripMargin)

}
