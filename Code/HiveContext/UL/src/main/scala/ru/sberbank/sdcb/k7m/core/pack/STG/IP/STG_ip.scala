package ru.sberbank.sdcb.k7m.core.pack.STG.IP

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class STG_ip(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_ip_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_ip_ShortName)

	val dataframe = spark.sql(
		s"""
			|select d.inn, -- ИНН
			|d.ip_flag,
			|d.fio, -- ФИО
			|d.birth_dt -- Дата рождения --, d.rn
			|from
			|(
			|        select
			|            b.inn
			|            ,case when b.rn = 1 then ip_flag end as ip_flag
			|            ,fio
			|            ,max(birth_dt) as birth_dt
			|            ,rn
			|        from
			|        (
			|            select a.*
			|            from
			|            (
			|                    SELECT distinct
			|                        trim(lower(concat(coalesce(concat(ip_last_nm,' '), ''),
			|                                             coalesce(concat(ip_first_nm,' '), ''),
			|                                             coalesce(ip_middle_nm, '')))) as fio
			|                        , ip_birth_dt as birth_dt
			|                        , ip_inn as inn
			|                        , CASE WHEN ip_active_flg=true THEN 1 ELSE 0 END as ip_flag
			|                        , ip_last_nm
			|                        , ip_first_nm
			|                        , ip_middle_nm
			|                        , ip_citizen_type
			|                        , ip_birth_place
			|                        , cast(ip_gender as string) as ip_gender
			|                        , row_number() over (partition by ip_inn, lower(ip_last_nm)
      |                                                 order by
			|                                                          effectivefrom desc
			|                                                        , effectiveto desc
			|                                                        , ip_slice_id desc
			|                                                        , load_dt desc
			|                                                        , ip_reg_first_dt desc
			|                                                        , ip_reg_ogrn_dt desc
			|                                                        , ip_pf_start_dt desc
			|                                                        , ip_org_id desc
			|                                                        , ip_citizen_type desc
			|                                                        , cast(ip_gender as string) desc
			|                                                        , ip_active_flg desc
			|                                                        ) as rn
			|                        , is_active_in_period
			|
			|                    FROM $ZeroLayerSchema.int_ip_organization_egrip
			|                    WHERE '$DATE' >= effectivefrom  --AND '$DATE' <= effectiveto
			|
			|
			|            ) a
			|
			|                where lower(a.is_active_in_period)='y'
			|                AND a.fio is not null AND fio<>''
			|                AND birth_dt is not null
			|                AND a.birth_dt<>''
			|                AND a.inn is not null
			|                AND length(a.inn)=12
			|                AND cast(a.inn as double) is not null
			|                AND a.inn not like '00000000%'
			|
			|        ) b
			|        group by b.inn, b.rn, b.fio, b.ip_flag
			|) d
			|where (d.rn = 1)
			|
			|
		""".stripMargin)
}
