package ru.sberbank.sdcb.k7m.core.pack.MRT3

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT3_GSL_FINAL(config: Config) extends Table(config: Config){

	import spark.implicits._

	val dashboardName: String = genDashBoardName(MRT3_GSL_FINAL_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT3_GSL_FINAL_ShortName)

	override def processName: String = "UL"

	private val MRT2_GSL_ALL = spark.table(genDashBoardName(MRT2_GSL_ALL_ShortName))

	private val custom_inn_table1 = spark.table(custom_inn).alias("custom_inn_table1")
	private val custom_inn_table2 = spark.table(custom_inn).alias("custom_inn_table2")

	private val mrt1_crta_5_1_7ip_Name  = genDashBoardName(MRT1_Crta_5_1_7ip_ShortName)
	private val dds_exec_Name = genDashBoardName(DDS_exec_ShortName)
	private val dds_dir_brd_Name = genDashBoardName(DDS_dir_brd_ShortName)

	private val mrt1_crta_5_1_7ip = spark.table(mrt1_crta_5_1_7ip_Name)
	private val dds_exec = spark.table(dds_exec_Name)
	private val dds_dir_brd = spark.table(dds_dir_brd_Name)

	val window_function: WindowSpec = Window.partitionBy($"criterion").orderBy($"inn1", $"inn2", $"crm_id1", $"crm_id2", $"quantity", $"fio1")


	val mrt1_crta_5_1_7ip_col = mrt1_crta_5_1_7ip
		.select(
			$"inn1",
			lit(null).cast(StringType).as("fio1"),
			lit(null).cast(StringType).as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			lit(null).cast(StringType).as("post"),
			lit(null).cast(StringType).as("crm_id1"),
			$"inn2",
			lit(null).cast(StringType).as("crm_id2"),
			$"criterion",
			$"confidence",
			$"quantity",
			$"dt",
			lit(null).cast(TimestampType).as("issue_dt")
		).withColumn("loc_id", concat($"criterion", lit("_"), row_number().over(window_function))).distinct

	val dds_exec_col = dds_exec
		.select(
			$"inn".as("inn1"),
			$"fio".as("fio1"),
			lit(null).cast(StringType).as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			$"post".cast(StringType).as("post"),
			lit(null).cast(StringType).as("crm_id1"),
			$"inn_established".as("inn2"),
			lit(null).cast(StringType).as("crm_id2"),
			lit("EIOGen").as("criterion"),
			lit(100).as("confidence"),
			lit(1).as("quantity"),
			lit(DATE).as("dt"),
			lit(null).cast(TimestampType).as("issue_dt")
		).withColumn("loc_id", concat(lit("exec_"), row_number().over(window_function))).distinct

	val dds_dir_brd_col = dds_dir_brd
		.select(
			lit(null).cast(StringType).as("inn1"),
			$"fio".as("fio1"),
			$"birth_year".as("birth_date1"),
			lit(null).cast(StringType).as("passport1"),
			lit(null).cast(StringType).as("mob_phone1"),
			$"bd_post_nm".as("post"),
			lit(null).cast(StringType).as("crm_id1"),
			$"inn_established".as("inn2"),
			lit(null).cast(StringType).as("crm_id2"),
			lit("EIOGen").as("criterion"),
			lit(100).as("confidence"),
			lit(1).as("quantity"),
			lit(DATE).as("dt"),
			lit(null).cast(TimestampType).as("issue_dt")
		).withColumn("loc_id", concat(lit("dir_"), row_number().over(window_function))).distinct

	val pre_dataframe = MRT2_GSL_ALL
		.join(custom_inn_table1, col("custom_inn_table1.org_inn_crm_num") === MRT2_GSL_ALL("inn1"), "left")
		.join(custom_inn_table2, col("custom_inn_table2.org_inn_crm_num") === MRT2_GSL_ALL("inn2"), "left")
  	.where(coalesce(col("custom_inn_table1.org_inn_crm_num"),col("custom_inn_table2.org_inn_crm_num")).isNotNull)
  	.select(MRT2_GSL_ALL("*"))

	val dataframe = pre_dataframe
		.union(mrt1_crta_5_1_7ip_col)
		.union(dds_exec_col)
  	.union(dds_dir_brd_col)


}
