package ru.sberbank.sdcb.k7m.core.pack.STG.FND_EG

import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_fnd_eg(config: Config) extends Table(config: Config) {

	val dashboardName: String = genDashBoardName(STG_fnd_eg_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_fnd_eg_ShortName)

	val stg_UL_Name: String = genDashBoardName(STG_UL_ShortName)

	import spark.implicits._



	val stg_fnd_eg_0: DataFrame = spark.sql(
			s"""select * from (
				|SELECT
				|    ofe.ul_org_id_founder
				|    , ofe.egrul_org_id_established
				|    , ofe.of_inn -- ИНН
				|    , ofe.of_share_rub_cnt
				|    , ofe.of_type
				|    , ofe.effectivefrom
				|    , ofe.of_nm  -- ФИО
				|    ,row_number() over (partition by egrul_org_id_established, of_inn order by effectivefrom desc
                                                                                      , of_actual_dt desc
                                                                                      , of_slice_id desc
                                                                                      , file_dt desc
                                                                                      , load_dt desc) as rn
				|FROM $ZeroLayerSchema.int_org_founder_egrul ofe
				|WHERE '$DATE' >= ofe.effectivefrom and '$DATE' <= ofe.effectiveto
				|    AND lower(ofe.is_active_in_period)='y'
				|    AND ofe.egrul_org_id_established is not null
				|    AND (
				|                (
				|                    (ofe.of_type=1)
				|                    AND (ofe.of_inn is not null)
				|                    AND (length(ofe.of_inn)=12)
				|                    AND (cast(ofe.of_inn as double) is not null)
				|                    AND (ofe.of_inn not like '00000000%')
				|                )
				|                OR (
				|                    (ofe.of_type=2)
				|                    AND (ofe.ul_org_id_founder is not null)
				|                    )
				|        )
				|    AND (ofe.of_share_rub_cnt is not null)
				|    AND cast(ofe.of_share_rub_cnt as string)!=''
        |    ) of2
        |     where of2.rn = 1
				""".stripMargin)

	stg_fnd_eg_0.createOrReplaceTempView("stg_fnd_eg_0")

//	val stg_fnd_eg_prep: DataFrame = spark.sql(
//		s"""
//	|    select
//	|        egrul_org_id_established, of_inn
//	|        from
//	|          stg_fnd_eg_0
//	|        group by egrul_org_id_established, of_inn\
//	|        having count(of_share_rub_cnt) > 1""".stripMargin)
//
//	stg_fnd_eg_prep.createOrReplaceTempView("stg_fnd_eg_prep")

	val stg_fnd_eg_bad: DataFrame = spark.sql(s"""
	|    select fnd.egrul_org_id_established, fnd.c_sum, eg.ul_capital_sum
	|    from
	|    (
	|        select
	|            cc.egrul_org_id_established, sum(cc.of_share_rub_cnt) as c_sum
	|            from
	|                   (
	|                       select distinct  egrul_org_id_established, of_inn, of_share_rub_cnt  from stg_fnd_eg_0
	|
	|                    ) cc
	|            group by cc.egrul_org_id_established
	|    ) fnd
	|    left join
	|            $stg_UL_Name eg
	|    on fnd.egrul_org_id_established = eg.egrul_org_id
	|
	|    where eg.ul_capital_sum < fnd.c_sum
	""".stripMargin)

		stg_fnd_eg_bad.createOrReplaceTempView("stg_fnd_eg_bad")

	val dataframe: DataFrame = spark.sql(s"""
	|select fnd.*
  |  from stg_fnd_eg_0 fnd
  |  left join stg_fnd_eg_bad bad
  |    on fnd.egrul_org_id_established = bad.egrul_org_id_established
  | where bad.egrul_org_id_established is null
    """.stripMargin)
}
