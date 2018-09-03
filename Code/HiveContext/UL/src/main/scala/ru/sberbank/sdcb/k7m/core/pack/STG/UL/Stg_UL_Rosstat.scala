package ru.sberbank.sdcb.k7m.core.pack.STG.UL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_UL_Rosstat(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_UL_Rosstat_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_UL_Rosstat_ShortName)

	val rosstat = spark.sql(s"""
    select
            a.ul_inn as inn
						,lower(a.ul_full_nm) as ul_full_nm
						,a.ul_org_id
						,a.UL_CAPITAL_SUM
						,a.ul_branch_cnt
						,a.egrul_org_id
						,a.ul_okopf_cd
						,'$DATE' as dt
						,a.is_active_in_period
       from (
                select
											 rank() over (partition by ul_org_id, egrul_org_id order by effectivefrom desc,
																																									ul_slice_id desc,
																																									effectiveto desc,
																																									UL_CAPITAL_SUM desc,
																																									nvl(ul_branch_cnt, 0) desc) as rn,
                       *
									from $ZeroLayerSchema.int_ul_organization_rosstat
                 where '$DATE' >= effectivefrom
                   and '$DATE' < effectiveto
                ) a
                where a.rn = 1
									and a.UL_CAPITAL_SUM > 0
									and a.UL_ACTIVE_FLG = true
									and nvl(a.ul_inn,'') <> ''
									AND lower(a.is_active_in_period)='y'
	""")

	rosstat.createOrReplaceTempView("rosstat")


	val adr_rosstat_uniq = spark.sql("""
		select inn, dt
		from rosstat
		group by inn, dt
		having count(ul_full_nm) = 1
	""")

	adr_rosstat_uniq.createOrReplaceTempView("rosstat_uniq")

	val adr_rosstat_clean = spark.sql(s"""
    select  c.inn,  c.ul_full_nm as ul_full_nm,  c.ul_org_id, c.UL_CAPITAL_SUM, c.ul_branch_cnt, c.dt, c.ul_okopf_cd, c.egrul_org_id
    from
    (
        select distinct
                    a.inn,
                    a.dt,
                    a.UL_CAPITAL_SUM,
                    lower(a.ul_full_nm) as ul_full_nm,
                    a.ul_org_id,
                    a.ul_branch_cnt,
                    a.ul_okopf_cd,
                    a.egrul_org_id,
                    row_number() over (partition by a.ul_org_id order by a.UL_CAPITAL_SUM desc, a.inn desc) as rn
        from rosstat a
        inner join
        (
            select inn, dt
            from rosstat
            group by inn, dt, UL_CAPITAL_SUM
            having count(ul_org_id) > 1
        ) b
        on a.inn = b.inn and a.dt = b.dt
        inner join $ZeroLayerSchema.int_financial_statement_rosstat v
        on a.ul_org_id = v.ul_org_id
        where a.ul_branch_cnt is not null
    ) c
    where c.rn = 1
	""")

	val adr_rosstat = spark.sql("""
    select  a.inn,  a.ul_full_nm,  a.ul_org_id, a.UL_CAPITAL_SUM, a.ul_branch_cnt, a.dt, a.ul_okopf_cd, a.egrul_org_id
    from rosstat a
    inner join rosstat_uniq b
    on a.inn = b.inn and a.dt = b.dt
""")

	val adr_rosstat_2 = adr_rosstat.union(adr_rosstat_clean)

	adr_rosstat_2.createOrReplaceTempView("rosstat2")

	val adr_rosstat_orgid =  spark.sql("""
	select a.*
	from rosstat2 a
	left join
	(
	    select inn
	    from rosstat2
	    group by inn
	    having count(distinct ul_org_id) > 1
	) b
	on a.inn = b.inn
	where (b.inn is null) or ((a.egrul_org_id is not null) and (b.inn is not null) )
	""")

	adr_rosstat_orgid.createOrReplaceTempView("rosstat3")

	val adr_rosstat_final = spark.sql("""
	select a.*
	from rosstat3 a
	left join
	(
	    select inn
	    from rosstat3
	    group by inn
	    having count(distinct egrul_org_id) > 1
	) b
	on a.inn = b.inn
	where (b.inn is null)
	""")

	val dataframe = adr_rosstat_final

}
