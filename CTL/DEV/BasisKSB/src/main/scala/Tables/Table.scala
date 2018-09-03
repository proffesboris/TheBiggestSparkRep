package Tables

import My_Utils.Variables
import org.apache.spark.sql.{DataFrame, SparkSession}
/** Class that will be inherited by every table, created in this application
	* Containing public variables and functions*/
abstract class Table extends Variables{

	/**
		* Variables to be defined in every class representing table
		* name: schema.name
		* dataframe: script for table creation in form of Dataset[Row]
		* SQLTtableStructure: Table fieldnames and types used to create empty hive table with structure of dataframe
		* saveFormat: format in which data will be saved in hdfs
		* */

	val name: String
	val dataframe: DataFrame
	val SQLTableStructure: String
	val saveFormat: String = "parquet"

	val spark: SparkSession = SparkSession.builder
		.appName("Basis_KSB")
		.enableHiveSupport()
		.getOrCreate

 	/** Variables - source tables*/
	val z_client: DataFrame     = spark.table(s"$SourceSchema.z_client")
	val z_ac_fin: DataFrame 		= spark.table(s"$SourceSchema.z_ac_fin")
	val z_main_docum: DataFrame = spark.table(s"$SourceSchema.z_main_docum")

	val z_ft_money1: DataFrame 			= spark.table(s"$SourceSchema.z_ft_money")
	val z_ft_money2: DataFrame 			= spark.table(s"$SourceSchema.z_ft_money")
	val z_name_paydoc: DataFrame 		= spark.table(s"$SourceSchema.z_name_paydoc")
	val z_kod_n_pay: DataFrame 			= spark.table(s"$SourceSchema.z_kod_n_pay")
	val z_branch: DataFrame				  = spark.table(s"$SourceSchema.z_branch")
	val z_sbrf_type_mess: DataFrame = spark.table(s"$SourceSchema.z_sbrf_type_mess")

	/** Function that drops and saves table. The one and only way working in PROM OD*/
	def save(): Unit = {
		val tempName = name.substring(SaveSchema.length + 1) + "_temp"
		dataframe.createOrReplaceTempView(s"$tempName")

		spark.sql(s"drop table if exists $name")
		spark.sql(s"create table $name($SQLTableStructure) stored as $saveFormat")
		spark.sql(s"insert into table $name select * from $tempName")
	}

	def drop(): Unit = {
		spark.sql(s"drop table if exists $name")
	}
}
