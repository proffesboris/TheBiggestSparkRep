package ru.sberbank.sdcb.k7m.core.pack.STG.TR_PRSN

import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class STG_tr_prsn(config: Config) extends Table(config: Config) {

	val dashboardName: String = genDashBoardName(STG_tr_prsn_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_tr_prsn_ShortName)

	val tp = spark.sql(
		s"""
			|
			|SELECT
			|        tp.egrul_org_id,
			|        tp.tp_inn, -- ИНН
			|        trim(lower(concat(coalesce(concat(tp.tp_last_nm,' '), ''),
			|                          coalesce(concat(tp.tp_first_nm,' '), ''),
			|                          coalesce(tp.tp_middle_nm, '')))) as fio, -- ФИО
			|        post,
			|        case when (post like '%ген%') and (
			|                                                    post not like '%зам%' and
			|                                                    post not like '%ликвид%' and
			|                                                    post not like '%секретарь%' and
			|                                                    post not like '%агент%' and
			|                                                    post not like '%глава%'  and
			|                                                    post not like '%руковод%' and
			|                                                    post not like '%специалист%' and
			|                                                    post not like '%конструктор%' and
			|                                                    post not like '%председатель тсж%' and
			|                                                    post not like '%комиссар%' and
			|                                                    post not like '%атаман%' and
			|                                                    post not like '%советник%' and
			|                                                    post not like '%помощник%'
			|                                                    ) then 1 else 0 end as gendir
			|
			|from
			|(
			|    select *
			|            ,row_number() over (partition by egrul_org_id, tp_inn order by effectivefrom desc) as rn
			|            ,lower(tp_post_nm) as post
			|    FROM $ZeroLayerSchema.int_trust_person
			|    WHERE '$DATE' >= effectivefrom and '$DATE' <= effectiveto
			|
			|)  tp
			|
			|where (length(tp.tp_inn)=12 )
			|        AND (tp.tp_inn not like '00000000%')
			|        AND (tp.tp_inn <> '')
			|        AND (lower(is_active_in_period)='y' )
			|        AND (tp.rn = 1)
			|        AND egrul_org_id is not null
			|        AND tp_inn is not null
			|
			|
			|
		""".stripMargin)

		tp.createOrReplaceTempView("tp")


	val dataframe: DataFrame = spark.sql(
		s"""
			|
			|select tp.*
			|from tp
			|
			|inner join
			|(
			|   select tp_inn  ,count(distinct egrul_org_id) as cnt
			|    from tp
			|    group by tp_inn
			|    having count(distinct egrul_org_id) < $TP_MAX_COMP
			|) b
			|on tp.tp_inn = b.tp_inn
			|
			|
		""".stripMargin)

}
