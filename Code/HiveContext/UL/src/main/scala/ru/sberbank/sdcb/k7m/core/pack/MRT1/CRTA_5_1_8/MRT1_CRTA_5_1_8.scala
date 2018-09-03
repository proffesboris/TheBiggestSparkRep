package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_8

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}
import org.apache.spark.sql.functions._

class MRT1_CRTA_5_1_8(config: Config) extends Table(config: Config){

	import spark.implicits._

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_8_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_8_ShortName)

	private val MRT1_crta_5_1_8_T5_Name = genDashBoardName(MRT1_Crta_5_1_8_T5_ShortName)

	private val prmrt4_extpownr_Name = genDashBoardName(PRMTR4_extpownr_ShortName)
	private val dds_rel_Name = genDashBoardName(DDS_rel_ShortName)

	private val MRT1_crta_5_1_8_T5_left  = spark.table(MRT1_crta_5_1_8_T5_Name).as("left_shares")
	private val MRT1_crta_5_1_8_T5_right = spark.table(MRT1_crta_5_1_8_T5_Name).as("right_shares")

	private val dds_rel = spark.table(dds_rel_Name)
	private val prmtr4_extpownr = spark.table(prmrt4_extpownr_Name)

	val pre_dataframe_part1 = MRT1_crta_5_1_8_T5_left.join(MRT1_crta_5_1_8_T5_right,
			col("left_shares.family_id") === col("right_shares.family_id") &&
			col("left_shares.inn2") =!= col("right_shares.inn2"),
		  "inner").groupBy(col("left_shares.inn2").as("inn1"), col("right_shares.inn2").as("inn2")).agg(
			lit("5.1.8").as("criterion"),
			lit(50).as("confidence"),
			sum(col("left_shares.quantity")).as("quantity"),
			lit(DATE).as("dt")
		).distinct

	val dataframe_part1 = pre_dataframe_part1.select($"inn1", $"inn2", $"criterion", $"confidence", $"quantity", $"dt")

	val dataframe_part2= prmtr4_extpownr.join(dds_rel,
		prmtr4_extpownr("family_id") === dds_rel("family_id") &&
		prmtr4_extpownr("inn1") =!= dds_rel("inn"),
		"inner").where(
		dds_rel("ip_flag") === 1
		).select(
		dds_rel("inn").as("inn1"),
		prmtr4_extpownr("inn2").as("inn2"),
		lit("5.1.8").as("criterion"),
		lit(50).as("confidence"),
		lit(0.0).as("quantity"),
		lit(DATE).as("dt")
	).distinct

	val dataframe = dataframe_part1.union(dataframe_part2)


	/*
	val dataframe = spark.sql(
		s"""
			|SELECT distinct
			|        left_shares.inn2 as inn1
			|        , right_shares.inn2 as inn2
			|        , '5.1.8' as criterion
			|        , 50 as confidence
			|        , sum(left_shares.quantity) as quantity
			|        , '$DATE' as dt
			|FROM $MRT1_crta_5_1_8_T5_Name left_shares
			|INNER JOIN $MRT1_crta_5_1_8_T5_Name right_shares
			|    on ((left_shares.family_id = right_shares.family_id) and (left_shares.inn2 <> right_shares.inn2))
			|GROUP BY left_shares.inn2, right_shares.inn2
		""".stripMargin)

*/
/*
	val dataframe_append = spark.sql(
		s"""
			|SELECT distinct
			|        r.inn as inn1
			|        , t.inn2 as inn2
			|        , '5.1.8' as criterion
			|        , 50 as confidence
			|        , 0 as quantity
			|        , '$DATE' as dt
			|FROM $prmrt4_extpownr_Name t
			|INNER JOIN $dds_rel_Name r
			|ON ((t.family_id=r.family_id) and (t.inn1 <> r.inn))
			|WHERE r.ip_flag=1
		""".stripMargin)

		*/

}
