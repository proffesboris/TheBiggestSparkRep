package ru.sberbank.sdcb.k7m.core.pack.DDS.OWNR

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class DDS_ownr(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(DDS_ownr_ShortName)
	val dashboardPath: String = genDashBoardPath(DDS_ownr_ShortName)

	private val Stg_UL_Name  = genDashBoardName(STG_UL_ShortName)
	private val Stg_shr_Name = genDashBoardName(STG_shr_ShortName)
	private val prdds2_fnd_Name = genDashBoardName(PRDDS2_fnd_ShortName)

	val dds_ownr_T1 = spark.sql(
		s"""
			|
			|    SELECT
			|            f.inn as inn_established, -- ИНН
			|            CASE WHEN f.of_type=1 THEN f.of_inn else ul2.inn END as inn,
			|            f.share_sum,
			|            f.of_type as share_type,
			|            f.of_nm as nm, -- ФИО
			|            ul1.ul_full_nm as nm_established
			|    FROM (
			|            SELECT
			|                    f1.inn
			|                    , cast(null as bigint) as ul_org_id_founder
			|                    , f1.of_inn
			|                    , f1.of_share_prcnt_cnt as share_sum
			|                    , f1.of_type
			|                    , f1.of_nm FROM $prdds2_fnd_Name f1 WHERE f1.of_type=1
			|        UNION ALL
			|            SELECT
			|                    f2.inn
			|                    , f2.ul_org_id_founder
			|                    , cast(null as string) as of_inn
			|                    , f2.of_share_prcnt_cnt as share_sum
			|                    , f2.of_type
			|                    , f2.of_nm FROM $prdds2_fnd_Name f2 WHERE f2.of_type=2
			|            ) f
			|    LEFT JOIN $Stg_UL_Name ul1
			|            ON (ul1.inn=f.inn)
			|    LEFT JOIN $Stg_UL_Name ul2
			|            ON (ul2.ul_org_id=f.ul_org_id_founder)
			|    WHERE  (lower(ul1.ul_kopf_nm) not like '%акционер%') AND (ul2.inn is not null or f.of_type=1)
			|
			|UNION ALL
			|    SELECT
			|        sh.sh_ul_inn as inn_established
			|        , sh.sh_inn as inn
			|        , sh.sh_share_prcnt_cnt as share_sum
			|        , sh.sh_type as share_type
			|        , sh.sh_nm as nm
			|        , ul1.ul_full_nm as nm_established
			|        FROM $Stg_shr_Name sh
			|    INNER JOIN $Stg_UL_Name ul1
			|        ON (ul1.ul_org_id=sh.ul_org_id)
			|    WHERE lower(ul1.ul_kopf_nm) like '%акционер%'
			|
			|
			|
			|
		""".stripMargin)

	dds_ownr_T1.createOrReplaceTempView("dds_ownr_T1")

	val dds_ownr_T2 = spark.sql(
		"""
			|
			|SELECT
			|    inn, -- ИНН
			|    inn_established,
			|    sum(share_sum) as share_sum,
			|    share_type,
			|    nm, -- ФИО
			|    nm_established
			|FROM dds_ownr_T1
			|GROUP BY inn, inn_established, share_type, nm, nm_established
			|
		""".stripMargin)

	dds_ownr_T2.createOrReplaceTempView("dds_ownr_T2")

	val dataframe = spark.sql(
		s"""
			|
			|SELECT
			|    shares.inn, -- ИНН
			|    shares.inn_established,
			|    shares.share_sum,
			|    shares.share_type,
			|    shares.nm -- ФИО
			|FROM dds_ownr_T2 shares
			|LEFT JOIN
			|    (SELECT
			|        inn_established,
			|        sum(share_sum)
			|        FROM dds_ownr_T2
			|        GROUP BY inn_established
			|        having sum(share_sum) > 1
			|    ) false_shares
			|    ON (shares.inn_established=false_shares.inn_established)
			|WHERE
			|    shares.inn!=shares.inn_established
			|    AND false_shares.inn_established is null
			|    AND shares.inn!='$NRD_INN'
			|    AND shares.inn_established!='$NRD_INN'
			|
			|
			|
		""".stripMargin)

}
