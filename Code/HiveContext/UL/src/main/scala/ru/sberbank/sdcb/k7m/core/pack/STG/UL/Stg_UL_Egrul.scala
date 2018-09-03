package ru.sberbank.sdcb.k7m.core.pack.STG.UL

import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_UL_Egrul(config: Config) extends Table(config: Config) {

	val dashboardName: String = genDashBoardName(STG_UL_Egrul_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_UL_Egrul_ShortName)

	val dataframe: DataFrame = spark.sql(s"""
		select d.*, lower(ul_head_post_nm) as post
		from
		(
				SELECT * , row_number() over (partition by ul_inn order by effectivefrom desc, effectiveto desc, ul_slice_id desc, ul_name_egrul_record_dt desc, file_dt desc, ul_org_id desc) as rn
				FROM $ZeroLayerSchema.int_ul_organization_egrul
				WHERE '$DATE' >= effectivefrom and '$DATE' < effectiveto

		) d
		where d.rn = 1
				AND d.ul_active_flg=true
				AND lower(d.is_active_in_period)='y'
		""")

}
