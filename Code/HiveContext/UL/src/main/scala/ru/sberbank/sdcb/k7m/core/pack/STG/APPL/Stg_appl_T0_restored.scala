package ru.sberbank.sdcb.k7m.core.pack.STG.APPL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Table}

class Stg_appl_T0_restored(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_appl_T0_restored_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_appl_T0_restored_ShortName)

	private val Stg_appl_T0_clean_pass_name = genDashBoardName(STG_appl_T0_clean_pass_ShortName)

	val dataframe = spark.sql(
		s"""
    |select distinct
    | case when d.fio = '' then cast(null as string) else d.fio end as fio
    | , d.inn
    | , case when d.birth_dt = '' then cast(null as string) else d.birth_dt end as birth_dt
    | , d.sys_recordkey
    | , d.t_629_a1_pass_num
    | , d.t_628_a1_pass_seria 
    |from 
    |(
    |select 
    |        case when (a.t_16887_a1_prev_pass_num is not null) and (a.t_16886_a1_prev_pass_seria is not null)
    |        then
    |            coalesce(
    |                     CASE WHEN 
    |                            length(a.t_512_a1_inn)<>12 
    |                            OR cast(a.t_512_a1_inn as double) is null 
    |                            OR a.t_512_a1_inn like '00000000%' 
    |                            OR a.t_512_a1_inn = '111111111111'
    |                            OR a.t_512_a1_inn = 'НЕ ПРЕДЪЯВЛЕ'
    |                            OR a.t_512_a1_inn = '554002077555'
    |                    THEN null 
    |                    else a.t_512_a1_inn END 
    |                  ,
    |                  b.inn) 
    |       else 
    |                     CASE WHEN 
    |                            length(a.t_512_a1_inn)<>12 
    |                            OR cast(a.t_512_a1_inn as double) is null 
    |                            OR a.t_512_a1_inn like '00000000%' 
    |                            OR a.t_512_a1_inn = '111111111111'
    |                            OR a.t_512_a1_inn = 'НЕ ПРЕДЪЯВЛЕ'
    |                            OR a.t_512_a1_inn = '554002077555'
    |                    THEN null
    |                    else a.t_512_a1_inn END 
    |       
    |       end as inn
    |     ,  
    |     case when (a.t_16887_a1_prev_pass_num is not null) and (a.t_16886_a1_prev_pass_seria is not null)
    |        then
    |            coalesce(
    |                     trim(lower(concat(coalesce(concat(a.t_511_a1_lname,' '), ''), 
    |                          coalesce(concat(a.t_555_a1_fname,' '), ''), 
    |                          coalesce(a.t_510_a1_sname, '')
    |                          )
    |                       )
    |                    ) 
    |                  ,
    |                  b.fio) 
    |       else 
    |                     trim(lower(concat(coalesce(concat(a.t_511_a1_lname,' '), ''), 
    |                          coalesce(concat(a.t_555_a1_fname,' '), ''), 
    |                          coalesce(a.t_510_a1_sname, '')
    |                          )
    |                   )
    |            )
    |       
    |       end as fio
    |     ,
    |     case when (a.t_16887_a1_prev_pass_num is not null) and (a.t_16886_a1_prev_pass_seria is not null)
    |        then
    |            coalesce(cast(to_date(cast(b.t_513_a1_dob as timestamp)) as string)
    |                  ,
    |                  b.t_513_a1_dob) 
    |       else 
    |                   cast(to_date(cast(b.t_513_a1_dob as timestamp)) as string)
    |       
    |       end as birth_dt  
    |     ,  a.sys_recordkey
    |     , a.t_629_a1_pass_num
    |     , a.t_628_a1_pass_seria
    |      
    |
    |from $ZeroLayerSchema.tsm_applicants a
    |left join $Stg_appl_T0_clean_pass_name b
    |on 
    |                    (b.t_629_a1_pass_num = a.t_16887_a1_prev_pass_num) 
    |                    AND (b.t_628_a1_pass_seria = a.t_16886_a1_prev_pass_seria)
    |) d
""".stripMargin)

}
