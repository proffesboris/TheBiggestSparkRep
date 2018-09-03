package ru.sberbank.sdcb.k7m.core.pack.tables

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.TimestampType
import ru.sberbank.sdcb.k7m.core.pack.{Basis_KSB_Main, Config}

class S9FinalBasis(config: Config, inn_source: String) extends Table(config: Config){

	val dashboardName: String = inn_source match {
		case "basis_client" => FinalBasisName1
		case "clu" => FinalBasisName2
	}
	val dashboardPath: String = inn_source match {
		case "basis_client" => FinalBasisPath1
		case "clu" => FinalBasisPath2
	}

	private val innsec = spark.table(ClientFilterEndName)
	private val table4 = spark.table(MainDocumJoinDictsName)

	private val innimport010617_5a = table4.join(broadcast(innsec),table4("c_kl_kt_1_1") === innsec("id"),"left")
		.select(
			table4("*"),
			coalesce(table4("c_kl_kt_2_INN"), innsec("c_inn")).alias("inn_sec"),
			innsec("c_name").alias("kl_2_org"))
		.where(table4("ktdt") === 1)

	private val innimport010617_5b = table4.join(broadcast(innsec),table4("c_kl_dt_1_1") === innsec("id"),"left")
		.select(
			table4("*"),
			coalesce(table4("c_kl_dt_2_INN"), innsec("c_inn")).alias("inn_sec"),
			innsec("c_name").alias("kl_2_org"))
		.where(table4("ktdt") === 0)

	val union = innimport010617_5a.union(innimport010617_5b)

	val dataframe: DataFrame = union.withColumn("hash_sum", md5(concat_ws(",",
		coalesce(col("c_acc_dt"), lit(0)),
		coalesce(col("c_acc_kt"), lit(0)),
		coalesce(col("c_date_prov"), unix_timestamp(lit("1900-01-01 00:00:00"), "YYYY-MM-dd HH:mm:ss").cast(TimestampType)),
		coalesce(col("c_kl_dt_2_1"), lit(0)),
		coalesce(col("c_kl_dt_2_2"), lit(0)),
		coalesce(col("c_kl_dt_2_inn"), lit(0)),
		coalesce(col("c_kl_kt_2_1"), lit(0)),
		coalesce(col("c_kl_kt_2_2"), lit(0)),
		coalesce(col("c_kl_kt_2_inn"), lit(0)),
		coalesce(col("c_nazn"), lit("X")),
		coalesce(col("c_num_dt"), lit("X")),
		coalesce(col("c_num_kt"), lit("X")),
		coalesce(col("c_sum"), lit(0)),
		coalesce(col("c_sum_nt"), lit(0)),
		coalesce(col("c_sum_po"), lit(0)),
		coalesce(col("c_type_mess"), lit(0)),
		coalesce(col("c_valuta"), lit(0)),
		coalesce(col("c_valuta_po"), lit(0)),
		coalesce(col("c_vid_doc"), lit(0))
	)))

}
