package ru.sberbank.sdcb.k7m.core.pack.PRDDS1.FND_EG

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRDDS1_fnd_eg(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRDDS1_fnd_eg_ShortName)
	val dashboardPath: String = genDashBoardPath(PRDDS1_fnd_eg_ShortName)

	private val Stg_fnd_eg_name = genDashBoardName(STG_fnd_eg_ShortName)
	private val Stg_ul_name 	  = genDashBoardName(STG_UL_ShortName)

	val dataframe = spark.sql(
		s"""
			|
			|
			|    SELECT
			|            fs.ul_org_id_founder,
			|            fs.egrul_org_id_established,
			|            fs.of_inn, -- ИНН
			|            fs.of_type,
			|            fs.of_nm, -- ФИО
			|            jnd.inn,
			|            CASE WHEN jnd.ul_capital_sum is null or jnd.sum_cap > jnd.ul_capital_sum
			|                     THEN (fs.of_share_rub_cnt/jnd.sum_cap)
			|                         ELSE (fs.of_share_rub_cnt/jnd.ul_capital_sum) END as of_share_prcnt_cnt
			|            ,  fs.effectivefrom
			|             ,  rank() over (partition by jnd.inn order by fs.effectivefrom desc) as rn
			|    FROM $Stg_fnd_eg_name fs
			|    INNER JOIN
			|        (
			|            SELECT agg.egrul_org_id_established, agg.sum_cap, u.ul_capital_sum, u.inn
			|            FROM
			|            (
			|                SELECT
			|                        f.egrul_org_id_established,
			|                        sum(f.of_share_rub_cnt) as sum_cap
			|                        FROM $Stg_fnd_eg_name f
			|                        GROUP BY f.egrul_org_id_established
			|                ) agg
			|            INNER JOIN $Stg_ul_name u
			|            ON (u.egrul_org_id = agg.egrul_org_id_established)
			|        ) jnd
			|    ON (fs.egrul_org_id_established = jnd.egrul_org_id_established)
			|    WHERE CASE WHEN jnd.ul_capital_sum is null or jnd.sum_cap > jnd.ul_capital_sum
			|               THEN (fs.of_share_rub_cnt/jnd.sum_cap) else (fs.of_share_rub_cnt/jnd.ul_capital_sum)
			|               END is not null
			|      and CASE WHEN jnd.ul_capital_sum is null or jnd.sum_cap > jnd.ul_capital_sum
			|               THEN (fs.of_share_rub_cnt/jnd.sum_cap) else (fs.of_share_rub_cnt/jnd.ul_capital_sum)
			|               END > 0.00000001
			|
			|
			|
		""".stripMargin)

}
