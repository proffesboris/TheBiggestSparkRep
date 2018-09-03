package ru.sberbank.sdcb.k7m.core.pack.tables

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.sberbank.sdcb.k7m.core.pack.{Basis_KSB_Main, Config, EtlJob, EtlLogger}

/** Class that will be inherited by every table, created in this application
	* Containing public variables and functions*/
abstract class Table(val config: Config) extends EtlLogger with EtlJob{

	/**
		* Variables to be defined in every class representing table
		* name: schema.name
		* dataframe: script for table creation in form of Dataset[Row]
		* SQLTtableStructure: Table fieldnames and types used to create empty hive table with structure of dataframe
		* saveFormat: format in which data will be saved in hdfs
		* */

	val dashboardName: String
	val dashboardPath: String
	val dataframe: DataFrame

	val spark: SparkSession = SparkSession.builder
		.appName("Basis_KSB")
		.enableHiveSupport()
		.getOrCreate



	override def processName: String = "BASIS TRAN EKS"

	val SaveSchema: String = config.pa
	val SourceSchema: String = config.stg

 	/** Variables - source tables*/
	val z_client: DataFrame     = spark.table(s"$SourceSchema.eks_z_client")
	val z_ac_fin: DataFrame 		= spark.table(s"$SourceSchema.eks_z_ac_fin")
	val z_main_docum: DataFrame = spark.table(s"$SourceSchema.eks_z_main_docum")

	val z_ft_money1: DataFrame 			= spark.table(s"$SourceSchema.eks_z_ft_money").alias("z_ft_money1")
	val z_ft_money2: DataFrame 			= spark.table(s"$SourceSchema.eks_z_ft_money").alias("z_ft_money2")
	val z_name_paydoc: DataFrame 		= spark.table(s"$SourceSchema.eks_z_name_paydoc")
	val z_kod_n_pay: DataFrame 			= spark.table(s"$SourceSchema.eks_z_kod_n_pay")
	val z_branch: DataFrame				  = spark.table(s"$SourceSchema.eks_z_branch")
	val z_sbrf_type_mess: DataFrame = spark.table(s"$SourceSchema.eks_z_sbrf_type_mess")

	/** Names of tables that are built inside this applications*/
	val ClientFilterBegName: String = SaveSchema + "." + "innimport190417_t17_1"
	val AcFinFilterName: String = SaveSchema + "." + "innimport190417_t17_2"
	val MainDocumFilterName: String = SaveSchema + "." + "innimport190417_t17_3"
	val MainDocumJoinDictsName: String = SaveSchema + "." + "innimport190417_t17_4"

	val DebitSelectName: String = SaveSchema + "." + "dt1_hive010617"
	val KreditSelectName: String = SaveSchema + "." + "kt1_hive010617"

	val DebitUnionKreditName: String = SaveSchema + "." + "distclient_hive010617"
	val ClientFilterEndName: String = SaveSchema + "." + "innsec_hive010617"

	val FinalBasisName1: String = SaveSchema + "." + "Basis_EKS"
	val FinalBasisName2: String = SaveSchema + "." + "Basis_EKS2"

	val ClientFilterBegPath: String = s"${config.paPath}innimport190417_t17_1"
	val AcFinFilterPath: String = s"${config.paPath}innimport190417_t17_2"
	val MainDocumFilterPath: String = s"${config.paPath}innimport190417_t17_3"
	val MainDocumJoinDictsPath: String = s"${config.paPath}innimport190417_t17_4"

	val DebitSelectPath: String = s"${config.paPath}dt1_hive010617"
	val KreditSelectPath: String = s"${config.paPath}kt1_hive010617"

	val DebitUnionKreditPath: String = s"${config.paPath}distclient_hive010617"
	val ClientFilterEndPath: String = s"${config.paPath}innsec_hive010617"

	val FinalBasisPath1: String = s"${config.paPath}Basis_EKS"
	val FinalBasisPath2: String = s"${config.paPath}Basis_EKS2"

	/** Function that drops and saves table. The one and only way working in PROM OD*/
	def save(): Unit = {
		spark.sql(s"drop table if exists $dashboardName")
		dataframe.write.format("parquet").mode("overwrite").option("path", dashboardPath).saveAsTable(s"$dashboardName")
	}

	def drop(): Unit = {
		spark.sql(s"drop table if exists $dashboardName")
	}

	def saveAndLog(): Unit = {
		logStart()
		save()
		logInserted()
		logEnd()
	}

}
