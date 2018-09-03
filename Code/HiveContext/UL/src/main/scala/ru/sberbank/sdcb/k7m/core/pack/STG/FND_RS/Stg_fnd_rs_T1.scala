package ru.sberbank.sdcb.k7m.core.pack.STG.FND_RS

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_fnd_rs_T1(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_fnd_rs_T1_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_fnd_rs_T1_ShortName)

	val dataframe = spark.sql(
		s"""
			 |SELECT
			 |        ofrs.ul_org_id_founder,
			 |        ofrs.ul_org_id_established,
			 |        cast(ofrs.of_share_prcnt_cnt as decimal(22,5)) / 100 as of_share_prcnt_cnt,
			 |        ofrs.of_type,
			 |        ofrs.effectivefrom,
			 |        ofrs.of_nm -- ФИО
			 |from
			 |(
			 |    select *, row_number() over (partition by ul_org_id_founder, ul_org_id_established order by effectivefrom desc, of_slice_id desc) as rn
			 |    FROM $ZeroLayerSchema.int_org_founder_rosstat
			 |    WHERE ('$DATE' >= effectivefrom) and '$DATE' <= effectiveto
			 |            AND (ul_org_id_founder is not null)
			 |            AND (ul_org_id_established is not null)
			 |            AND (of_type=2)
			 |) ofrs
			 |WHERE
			 |     lower(ofrs.is_active_in_period)='y'
			 |    AND ofrs.of_share_prcnt_cnt is not null
			 |    AND cast(ofrs.of_share_prcnt_cnt as string)!=''
			 |   and ofrs.rn = 1
			|
		""".stripMargin)



}
