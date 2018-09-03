package ru.sberbank.sdcb.k7m.core.pack.STG.APPL

import ru.sberbank.sdcb.k7m.core.pack.{Config, Table}

class Stg_appl_T0_clean_pass(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_appl_T0_clean_pass_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_appl_T0_clean_pass_ShortName)

	val dataframe = spark.sql(
		s"""
 		|
 		|select distinct
 		|        b.t_629_a1_pass_num
 		|        , b.t_628_a1_pass_seria
 		|        , trim(lower(concat(coalesce(concat(b.t_511_a1_lname,' '), ''),
 		|                          coalesce(concat(b.t_555_a1_fname,' '), ''),
 		|                          coalesce(b.t_510_a1_sname, '')
 		|                          )
 		|                   )
 		|            ) as fio
 		|        , cast(to_date(cast(b.t_513_a1_dob as timestamp)) as string) as t_513_a1_dob
 		|        ,   CASE WHEN
 		|                length(b.t_512_a1_inn)<>12
 		|                OR (cast(b.t_512_a1_inn as double) is null)
 		|                OR (b.t_512_a1_inn like '00000000%')
 		|               OR (b.t_512_a1_inn in ('111111111111', 'НЕ ПРЕДЪЯВЛЕ', '554002077555') )
 		|
 		|            then cast(null as string)
 		|            else b.t_512_a1_inn
 		|            end
 		|            as inn
 		|
 		|
 		|from   $ZeroLayerSchema.tsm_applicants b
 		|
 		|
 		|inner join
 		|(
 		|
 		|    select
 		|                a.t_629_a1_pass_num
 		|                , a.t_628_a1_pass_seria
 		|                , count( distinct trim(lower(
 		|                                            concat(coalesce(concat(a.t_511_a1_lname,' '), ''),
 		|                                                   coalesce(concat(a.t_555_a1_fname,' '), ''),
 		|                                                   coalesce(a.t_510_a1_sname, '')
 		|                                                   )
 		|                                             )
 		|                                       )
 		|                        ) as cnt_fio
 		|                , count( distinct cast(to_date(cast(a.t_513_a1_dob as timestamp)) as string)) as cnt_dt
 		|                , count( distinct nvl(t_512_a1_inn, '')) as cnt_inn
 		|
 		|    from  $ZeroLayerSchema.tsm_applicants a
 		|    where (nvl(a.t_629_a1_pass_num, '') <> '') and (nvl(a.t_628_a1_pass_seria, '') <> '')
 		|    group by a.t_629_a1_pass_num, a.t_628_a1_pass_seria
 		|) c
 		|on (b.t_629_a1_pass_num = c.t_629_a1_pass_num) and (b.t_628_a1_pass_seria = c.t_628_a1_pass_seria)
 		|where
 		| (c.cnt_fio = 1)
 		|     and (c.cnt_inn = 1)
 		|     and (c.cnt_dt = 1)
 		|     and (nvl(b.t_629_a1_pass_num, '') not in  ('', '0000', '0', '00000', '000000', '1'))
 		|     and (length(b.t_629_a1_pass_num) = 6)
 		|     and (nvl(b.t_628_a1_pass_seria, '') not in ('', '0000', '0', '1'))
 		|     and (
 		|             (nvl(b.t_512_a1_inn, '') <> '')
 		|             or
 		|             (
 		|                  (
 		|                  trim(lower(
 		|                                                concat(coalesce(concat(b.t_511_a1_lname,' '), ''),
 		|                                                       coalesce(concat(b.t_555_a1_fname,' '), ''),
 		|                                                       coalesce(b.t_510_a1_sname, '')
 		|                                                       )
 		|                                                 )
 		|                                           ) <> ''
 		|                    )
 		|                 and
 		|                  (
 		|
 		|                cast(to_date(cast(b.t_513_a1_dob as timestamp)) as string) <> ''
 		|                 )
 		|             )
 		|
 		|     )
""".stripMargin)

}
