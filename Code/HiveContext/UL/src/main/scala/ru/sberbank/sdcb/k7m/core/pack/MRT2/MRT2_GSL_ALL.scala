package ru.sberbank.sdcb.k7m.core.pack.MRT2

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, StringType, TimestampType}

class MRT2_GSL_ALL(config: Config) extends Table(config: Config){

	import spark.implicits._

	val dashboardName: String = genDashBoardName(MRT2_GSL_ALL_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT2_GSL_ALL_ShortName)

	override def processName: String = "UL"

	private val mrt1_crta_5_1_1_Name 	 = genDashBoardName(MRT1_Crta_5_1_1_ShortName)
	private val mrt1_crta_5_1_4_Name 	 = genDashBoardName(MRT1_Crta_5_1_4_ShortName)
	private val mrt1_crta_5_1_5_Name 	 = genDashBoardName(MRT1_Crta_5_1_5_ShortName)
	private val mrt1_crta_5_1_6_Name 	 = genDashBoardName(MRT1_Crta_5_1_6_ShortName)
	private val mrt1_crta_5_1_7_Name 	 = genDashBoardName(MRT1_Crta_5_1_7_ShortName)
	//private val mrt1_crta_5_1_7ip_Name  = genDashBoardName(MRT1_Crta_5_1_7ip_ShortName)
	private val mrt1_crta_5_1_7ben_Name = genDashBoardName(MRT1_Crta_5_1_7ben_ShortName)
	private val mrt1_crta_5_1_8_Name 	 = genDashBoardName(MRT1_Crta_5_1_8_ShortName)
	private val mrt1_crta_5_1_9_Name 	 = genDashBoardName(MRT1_Crta_5_1_9_ShortName)

	private val stg_ul_Name = genDashBoardName(STG_UL_ShortName)

	private val mrt1_crta_5_1_1 = spark.table(mrt1_crta_5_1_1_Name)
	private val mrt1_crta_5_1_4 = spark.table(mrt1_crta_5_1_4_Name)
	private val mrt1_crta_5_1_5 = spark.table(mrt1_crta_5_1_5_Name)
	private val mrt1_crta_5_1_6 = spark.table(mrt1_crta_5_1_6_Name)
	private val mrt1_crta_5_1_7 = spark.table(mrt1_crta_5_1_7_Name)
	//private val mrt1_crta_5_1_7ip = spark.table(mrt1_crta_5_1_7ip_Name)
	private val mrt1_crta_5_1_7ben = spark.table(mrt1_crta_5_1_7ben_Name)
	private val mrt1_crta_5_1_8 = spark.table(mrt1_crta_5_1_8_Name)
	private val mrt1_crta_5_1_9 = spark.table(mrt1_crta_5_1_9_Name)


	private val stg_ul1 = spark.table(stg_ul_Name).alias("ul1")
	private val stg_ul2 = spark.table(stg_ul_Name).alias("ul2")

	private val ben_crm = spark.table(genDashBoardName(CRM_BENFR_TABLE_ShortName)).where($"fz_ben".isNotNull)

	val mrt1_crta_5_1_1_col = mrt1_crta_5_1_1
		.select(
			$"inn1",
			lit(null).cast(StringType).as("fio1"),
			lit(null).cast(StringType).as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			lit(null).cast(StringType).as("post"),
			$"inn2",
			$"criterion",
			$"confidence",
			$"quantity",
			$"dt",
			lit(null).cast(StringType).as("crm_id1"),
			lit(null).cast(StringType).as("crm_id2"),
			lit(null).cast(TimestampType).as("issue_dt")
		)

	val mrt1_crta_5_1_4_col = mrt1_crta_5_1_4
		.select(
			$"inn1",
			lit(null).cast(StringType).as("fio1"),
			lit(null).cast(StringType).as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			lit(null).cast(StringType).as("post"),
			$"inn2",
			$"criterion",
			$"confidence",
			$"quantity",
			$"dt",
			lit(null).cast(StringType).as("crm_id1"),
			lit(null).cast(StringType).as("crm_id2"),
			lit(null).cast(TimestampType).as("issue_dt")
		)

	val mrt1_crta_5_1_5_col = mrt1_crta_5_1_5
		.select(
			$"inn1",
			lit(null).cast(StringType).as("fio1"),
			lit(null).cast(StringType).as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			lit(null).cast(StringType).as("post"),
			$"inn2",
			$"criterion",
			$"confidence",
			$"quantity",
			$"dt",
			lit(null).cast(StringType).as("crm_id1"),
			lit(null).cast(StringType).as("crm_id2"),
			lit(null).cast(TimestampType).as("issue_dt")
		)

	val mrt1_crta_5_1_6_col = mrt1_crta_5_1_6
		.select(
			$"inn1",
			$"fio".as("fio1"),
			lit(null).cast(StringType).as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			lit(null).cast(StringType).as("post"),
			$"inn2",
			$"criterion",
			$"confidence",
			$"quantity",
			$"dt",
			lit(null).cast(StringType).as("crm_id1"),
			lit(null).cast(StringType).as("crm_id2"),
			lit(null).cast(TimestampType).as("issue_dt")
		)

	val mrt1_crta_5_1_7_col = mrt1_crta_5_1_7
		.select(
			$"inn1",
			lit(null).cast(StringType).as("fio1"),
			lit(null).cast(StringType).as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			lit(null).cast(StringType).as("post"),
			$"inn2",
			$"criterion",
			$"confidence",
			$"quantity",
			$"dt",
			lit(null).cast(StringType).as("crm_id1"),
			lit(null).cast(StringType).as("crm_id2"),
			lit(null).cast(TimestampType).as("issue_dt")
		)

	val mrt1_crta_5_1_7ben_col = mrt1_crta_5_1_7ben
		.select(
			$"inn1",
			lit(null).cast(StringType).as("fio1"),
			lit(null).cast(StringType).as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			lit(null).cast(StringType).as("post"),
			$"inn2",
			$"criterion",
			$"confidence",
			$"quantity",
			$"dt",
			lit(null).cast(StringType).as("crm_id1"),
			lit(null).cast(StringType).as("crm_id2"),
			lit(null).cast(TimestampType).as("issue_dt")
		)

	val mrt1_crta_5_1_8_col = mrt1_crta_5_1_8
		.select(
			$"inn1",
			lit(null).cast(StringType).as("fio1"),
			lit(null).cast(StringType).as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			lit(null).cast(StringType).as("post"),
			$"inn2",
			$"criterion",
			$"confidence",
			$"quantity",
			$"dt",
			lit(null).cast(StringType).as("crm_id1"),
			lit(null).cast(StringType).as("crm_id2"),
			lit(null).cast(TimestampType).as("issue_dt")
		)

	val mrt1_crta_5_1_9_col = mrt1_crta_5_1_9
		.select(
			$"inn1",
			lit(null).cast(StringType).as("fio1"),
			lit(null).cast(StringType).as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			lit(null).cast(StringType).as("post"),
			$"inn2",
			$"criterion",
			$"confidence",
			$"quantity",
			$"dt",
			lit(null).cast(StringType).as("crm_id1"),
			lit(null).cast(StringType).as("crm_id2"),
			lit(null).cast(TimestampType).as("issue_dt")
		)


	val ben_crm_col = ben_crm
  	.select(
			$"inn_enrich".as("inn1"),
			trim(
				lower(
					concat(
					coalesce($"last_name", lit(" ")), lit(" "),
					coalesce($"fst_name", lit(" ")), lit(" "),
					coalesce($"mid_name", lit(" "))
					)
				)
			).as("fio1"),
			$"birth_dt".as("birth_date1"),
			concat(coalesce($"series", lit(" ")),
				coalesce($"doc_number", lit(" "))).as("passport1"),
			$"cell_ph_num".as("mob_phone1"),
			lit(null).cast(StringType).as("post"),
			$"inn".alias("inn2"),
			$"crit".as("criterion"),
			$"link_prob".as("confidence"),
			$"quantity".as("quantity"),
			lit(DATE).as("dt"),
			$"fz_ben".as("crm_id1"),
			$"crm_id".as("crm_id2"),
			$"issue_dt"
		)

	val window_function: WindowSpec = Window.partitionBy($"criterion").orderBy($"inn1", $"inn2", $"crm_id1", $"crm_id2", $"quantity", $"fio1")

	val pre_dataframe = mrt1_crta_5_1_1_col
		.union(mrt1_crta_5_1_4_col)
		.union(mrt1_crta_5_1_5_col)
  	.union(mrt1_crta_5_1_6_col)
		.union(mrt1_crta_5_1_7_col)
		//.union(mrt1_crta_5_1_7ip_col)
		.union(mrt1_crta_5_1_7ben_col)
		.union(mrt1_crta_5_1_8_col)
		.union(mrt1_crta_5_1_9_col)
  	.union(ben_crm_col)


	val pre_dataframe_joined = pre_dataframe
		.join(stg_ul1, col("ul1.inn") === pre_dataframe("inn1"), "left")
	  .join(stg_ul2, col("ul2.inn") === pre_dataframe("inn2"), "left")
  	.where(
			!(coalesce(stg_ul1("rs_okopf_cd"), lit("")) === "7.16.01") &&
			!(coalesce(stg_ul2("rs_okopf_cd"), lit("")) === "7.16.01") &&
			!(coalesce(stg_ul1("rs_okopf_cd"), lit("")) === "7.51.04") &&
			!(coalesce(stg_ul2("rs_okopf_cd"), lit("")) === "7.51.04") &&
				(!(pre_dataframe("inn1") === "7707083893") || pre_dataframe("criterion") === "ben_crm") &&
			!(pre_dataframe("inn2") === "7707083893")
		)

	val dataframe = pre_dataframe_joined.select(
		$"inn1",
		$"fio1",
		$"birth_date1",
		$"passport1",
		$"mob_phone1",
		$"post",
		$"crm_id1",
		$"inn2",
		$"crm_id2",
		$"criterion",
		$"confidence",
		$"quantity",
		$"dt",
		$"issue_dt"
	).withColumn("loc_id", concat($"criterion", lit("_"), row_number().over(window_function))).distinct

}
