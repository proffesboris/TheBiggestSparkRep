package ru.sberbank.sdcb.k7m.core.pack.STG.UL

import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_UL(config: Config) extends Table(config: Config) {

	val dashboardName: String = genDashBoardName(STG_UL_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_UL_ShortName)

	private val stg_UL_Egrul_Name   = genDashBoardName(STG_UL_Egrul_ShortName)
	private val stg_UL_Rosstat_Name =  genDashBoardName(STG_UL_Rosstat_ShortName)

	val dataframe: DataFrame = spark.sql(s"""

	SELECT
					eg.ul_org_id
					,eg.egrul_org_id
					,eg.ul_inn as inn
					,rs.ul_okopf_cd as rs_okopf_cd
					,eg.ul_full_nm
					,eg.ul_capital_sum
					,ref.okopf_nm as ul_kopf_nm
					,eg.ul_head_inn as eg_ul_head_inn
					,eg.ul_head_post_nm as eg_ul_head_nm
					,eg.ul_head_last_nm as eg_ul_last_nm
					,eg.ul_head_first_nm as eg_ul_first_nm
					,eg.ul_head_middle_nm as eg_ul_middle_nm
					,trim(lower(concat(coalesce(concat(eg.ul_head_last_nm,' '), ''),
														 coalesce(concat(eg.ul_head_first_nm,' '), ''),
														 coalesce(eg.ul_head_middle_nm,'')))) as head_fio
					, case when (eg.post like '%ген%') and (
																											eg.post not like '%зам%' and
																											eg.post not like '%ликвид%' and
																											eg.post not like '%секретарь%' and
																											eg.post not like '%агент%' and
																											eg.post not like '%глава%'  and
																											eg.post not like '%руковод%' and
																											eg.post not like '%специалист%' and
																											eg.post not like '%конструктор%' and
																											eg.post not like '%председатель тсж%' and
																											eg.post not like '%комиссар%' and
																											eg.post not like '%атаман%' and
																											eg.post not like '%советник%' and
																											eg.post not like '%помощник%'
																											) then 1 else 0 end as gendir
	FROM
	$stg_UL_Egrul_Name eg
	LEFT JOIN
	$stg_UL_Rosstat_Name rs

	ON ( rs.inn = eg.ul_inn and rs.ul_org_id=eg.ul_org_id) AND rs.egrul_org_id=eg.egrul_org_id
	LEFT JOIN $ZeroLayerSchema.int_ref_okopf ref
	ON (ref.okopf_cd=rs.ul_okopf_cd)
	WHERE eg.ul_inn is not null AND length(eg.ul_inn)=10 AND cast(eg.ul_inn as double) is not null AND eg.ul_inn not like '00000000%'
	""")

}
