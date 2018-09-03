package ru.sberbank.sdcb.k7m.core.pack.PRMTR3.OWNR_UL

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class PRMTR3_ownr_ul(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(PRMTR3_ownr_ul_ShortName)
	val dashboardPath: String = genDashBoardPath(PRMTR3_ownr_ul_ShortName)

	val PRMRT2_OWNR_RES_T3_name = genDashBoardName(PRMTR2_ownr_res_t3_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT
			|        inn as inn1
			|        , inn_established as inn2
			|        , share_sum
			|        , max_share_flag
			|
			|FROM $PRMRT2_OWNR_RES_T3_name
			|
			|WHERE share_type = 2
		""".stripMargin)



//	val PRMTR2_ownr_res_Name:String = genDashBoardName(PRMTR2_ownr_res_ShortName)
//
//	import spark.implicits._
//
//	val window_function_max: WindowSpec = Window.partitionBy("inn_established").orderBy($"share_sum".desc)
//
//	val PRMTR2_ownr_res = spark.table(PRMTR2_ownr_res_Name)
//
//	val PRMTR2_ownr_res_windowed = PRMTR2_ownr_res.withColumn("max_share_sum", max($"share_sum").over(window_function_max))
//
//	val shares_count = PRMTR2_ownr_res_windowed
//		.groupBy($"inn_established", $"share_sum")
//		.agg(count("*").as("cnt"))
//		.withColumn("max_share_sum", max($"share_sum").over(window_function_max))
//  	.where($"share_sum" === $"max_share_sum"&& $"share_sum" < 0.2 && $"share_sum" > 0.01)
//  	.select($"inn_established".as("inn_company"), $"cnt")
//
//	shares_count.createOrReplaceTempView("shares_count")
//
//	val pre_dataframe = PRMTR2_ownr_res_windowed.join(spark.table("shares_count"), PRMTR2_ownr_res_windowed("inn_established") === col("shares_count.inn_company"), "left")
//		.select(
//		$"inn".as("inn1"),
//		$"inn_established".as("inn2"),
//		$"share_sum",
//		$"cnt",
//		when($"share_sum" === $"max_share_sum", lit(1)).otherwise(lit(0)).as("max_share_flag")
//	).where($"share_type" === "2")
//
//	val dataframe = pre_dataframe.withColumn("max_share_flag", when($"cnt" > 1, lit(0)).otherwise($"max_share_flag")).drop("cnt")

/*
	val dataframe = spark.sql(
		s"""
			|SELECT
			|        res.inn as inn1 --ИНН
			|
			|        , res.inn_established as inn2
			|        , res.share_sum
			|        , CASE WHEN res.rn = 1 THEN 1 else 0 END as max_share_flag
			|FROM
			|    (
			|            SELECT *
			|                    , row_number() over (partition by inn_established order by share_sum desc) as rn
			|            FROM $PRMTR2_ownr_res_Name
			|    ) res
			|WHERE res.share_type = 2
		""".stripMargin)
*/
}
