package ru.sberbank.sdcb.k7m.core.pack.PRDDS2.FND

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRDDS2_fnd(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRDDS2_fnd_ShortName)
	val dashboardPath: String = genDashBoardPath(PRDDS2_fnd_ShortName)

	private val PRDDS1_fnd_eg = genDashBoardName(PRDDS1_fnd_eg_ShortName)
	private val PRDDS1_fnd_rs = genDashBoardName(PRDDS1_fnd_rs_ShortName)


	val PRDDS2_fnd_T1_name: String = genDashBoardName(PRDDS2_fnd_T1_ShortName)
	val PRDDS2_fnd_T1_path: String = genDashBoardPath(PRDDS2_fnd_T1_ShortName)


	val PRDDS2_fnd_T1 = spark.sql(s"""
SELECT
    eg.inn as eg_inn
    , rs.inn as rs_inn
    , CASE WHEN rs.inn is not null THEN 'rs' else 'eg'  END as label
FROM
    (SELECT * --distinct inn
    FROM $PRDDS1_fnd_eg where rn=1) eg
FULL JOIN
    (SELECT * --distinct inn
    FROM $PRDDS1_fnd_rs where rn=1) rs
on (eg.inn=rs.inn)
""")

	save(PRDDS2_fnd_T1, PRDDS2_fnd_T1_name, PRDDS2_fnd_T1_path)

	PRDDS2_fnd_T1.createOrReplaceTempView("prdds2_fnd_T1")

	val dataframe = spark.sql(
		s"""
			|
			|    SELECT
			|        ul_org_id_founder
			|        , egrul_org_id_established
			|        , of_inn -- ИНН
			|        , of_type
			|        , of_share_prcnt_cnt
			|        , of_nm -- ФИО
			|        , inn
			|    FROM $PRDDS1_fnd_eg eg
			|    INNER JOIN prdds2_fnd_T1 t1
			|    on (eg.inn=t1.eg_inn)
			|    WHERE lower(t1.label)='eg'
			|UNION ALL
			|    SELECT
			|        ul_org_id_founder
			|        , egrul_org_id_established
			|        , of_inn -- ИНН
			|        , of_type
			|        , of_share_prcnt_cnt
			|        , of_nm -- ФИО
			|        , inn
			|    FROM $PRDDS1_fnd_rs rs
			|    INNER JOIN prdds2_fnd_T1 t1
			|    on (rs.inn=t1.rs_inn)
			|    WHERE lower(t1.label)='rs'
			|
			|
		""".stripMargin)

}
