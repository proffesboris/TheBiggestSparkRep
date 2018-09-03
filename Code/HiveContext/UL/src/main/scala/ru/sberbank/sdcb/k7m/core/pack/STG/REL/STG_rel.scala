package ru.sberbank.sdcb.k7m.core.pack.STG.REL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class STG_rel(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_rel_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_rel_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT DISTINCT
			|             trim(lower(concat(coalesce(concat(r.t_245_a2_lname,' '), ''),
			|                               coalesce(concat(r.t_243_a2_fname,' '), ''),
			|                               coalesce(r.t_244_a2_sname,'')))) as fio2
			|            , cast(to_date(cast(r.t_246_a2_dob as timestamp)) as string) as birth_dt2
			|
			|            , trim(lower(concat(coalesce(concat(r.t_1498_a3_lname,' '), ''),
			|                  coalesce(concat(r.t_1496_a3_fname,' '), ''),
			|                  coalesce(r.t_1497_a3_sname,'')))) as fio3
			|            , cast(to_date(cast(r.t_1499_a3_dob as timestamp)) as string) as birth_dt3
			|
			|            , trim(lower(concat(coalesce(concat(r.t_1505_a4_lname, ' '), ''),
			|                                coalesce(concat(r.t_1503_a4_fname,' '), ''),
			|                                coalesce(r.t_1504_a4_sname,'')))) as fio4
			|            , cast(to_date(cast(r.t_1506_a4_dob as timestamp)) as string) as birth_dt4
			|
			|            , trim(lower(concat(coalesce(concat(r.t_265_a5_lname,' '), ''),
			|                                coalesce(concat(r.t_263_a5_fname,' '), ''),
			|                                coalesce(r.t_264_a5_sname,'')))) as fio5
			|            , cast(to_date(cast(r.t_266_a5_dob as timestamp)) as string) as birth_dt5
			|
			|            , trim(lower(concat(coalesce(concat(r.t_271_a6_lname,' '), ''),
			|                                coalesce(concat(r.t_269_a6_fname,' '), ''),
			|                                coalesce(r.t_270_a6_sname,'')))) as fio6
			|            , cast(to_date(cast(r.t_272_a6_dob as timestamp)) as string) as birth_dt6
			|
			|            , trim(lower(concat(coalesce(concat(r.t_277_a7_lname,' '), ''),
			|                                coalesce(concat(r.t_275_a7_fname,' '), ''),
			|                                coalesce(r.t_276_a7_sname,'')))) as fio7
			|            , cast(to_date(cast(r.t_278_a7_dob as timestamp)) as string) as birth_dt7
			|
			|            , trim(lower(concat(coalesce(concat(r.t_283_a8_lname,' '), ''),
			|                                coalesce(concat(r.t_281_a8_fname,' '), ''),
			|                                coalesce(r.t_282_a8_sname,'')))) as fio8
			|            , cast(to_date(cast(r.t_284_a8_dob as timestamp)) as string) as birth_dt8
			|
			|            , trim(lower(concat(coalesce(concat(r.t_289_a9_lname,' '), ''),
			|                                coalesce(concat(r.t_287_a9_fname,' '), ''),
			|                                coalesce(r.t_288_a9_sname,'')))) as fio9
			|            , cast(to_date(cast(r.t_290_a9_dob as timestamp)) as string) as birth_dt9
			|
			|            , trim(lower(concat(coalesce(concat(r.t_295_a10_lname,' '), ''),
			|                                coalesce(concat(r.t_293_a10_fname,' '), ''),
			|                                coalesce(r.t_294_a10_sname,'')))) as fio10
			|            , cast(to_date(cast(r.t_296_a10_dob as timestamp)) as string) as birth_dt10
			|
			|            , trim(lower(concat(coalesce(concat(r.t_1534_a11_lname,' '), ''),
			|                                coalesce(concat(r.t_1532_a11_fname,' '), ''),
			|                                coalesce(r.t_1533_a11_sname,'')))) as fio11
			|            , cast(to_date(cast(r.t_1535_a11_dob as timestamp)) as string) as birth_dt11
			|
			|            , trim(lower(concat(coalesce(concat(r.t_1542_a12_lname,' '), ''),
			|                                coalesce(concat(r.t_1540_a12_fname,' '), ''),
			|                                coalesce(r.t_1541_a12_sname,'')))) as fio12
			|            , cast(to_date(cast(r.t_1543_a12_dob as timestamp)) as string) as birth_dt12
			|
			|            , trim(lower(concat(coalesce(concat(r.t_1549_a13_lname,' '), ''),
			|                                coalesce(concat(r.t_1547_a13_fname,' '), ''),
			|                                coalesce(r.t_1548_a13_sname,'')))) as fio13
			|            , cast(to_date(cast(r.t_1550_a13_dob as timestamp)) as string) as birth_dt13
			|
			|            , trim(lower(concat(coalesce(concat(r.t_1557_a14_lname,' '), ''),
			|                                coalesce(concat(r.t_1555_a14_fname,' '), ''),
			|                                coalesce(r.t_1556_a14_sname,'')))) as fio14
			|            , cast(to_date(cast(r.t_1558_a14_dob as timestamp)) as string) as birth_dt14
			|
			|            , r.sys_recordkey
			|FROM $ZeroLayerSchema.tsm_relatives r
		""".stripMargin)

}
