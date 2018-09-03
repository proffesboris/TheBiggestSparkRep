package ru.sberbank.sdcb.k7m.core.pack.tables

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Basis_KSB_Main, Config}


class S3MainDocumFilter(config: Config, inn_source: String) extends Table(config: Config){

	val dashboardName: String = MainDocumFilterName
	val dashboardPath: String = MainDocumFilterPath

	import spark.implicits._

	private val inn_table = inn_source match {
		case "basis_client" => spark.table(s"${config.aux}.basis_client").select($"org_inn_crm_num".alias("inn"))
		case "clu" =>  spark.table(s"${config.pa}.clu").select($"inn".alias("inn"))
	}

	private val table2 = spark.table(AcFinFilterName)

	val dataframe: DataFrame = z_main_docum
		.join(broadcast(table2), z_main_docum("c_acc_kt") === table2("id"), "left")
		.join(broadcast(inn_table), z_main_docum("C_KL_KT_2_INN") === inn_table("inn"), "left") //20180502 Ключко Исправил inner на left т.к. не попадали проводки
		.select(
			z_main_docum("id")
			,lit("0").alias("ktdt")
			,z_main_docum("class_id")
			,z_main_docum("state_id")
			,z_main_docum("c_acc_dt")
			,z_main_docum("c_acc_kt")
			//,from_unixtime((z_main_docum("c_date_prov") / 1000).cast("Int")).alias("c_date_prov")
			,z_main_docum("c_date_prov")
			,z_main_docum("c_kl_dt_1_1")
			,z_main_docum("c_kl_dt_1_2")
			,z_main_docum("c_kl_dt_2_1")
			,z_main_docum("c_kl_dt_2_inn")
			,z_main_docum("c_kl_dt_2_2")
			,z_main_docum("c_kl_dt_2_3")
			,z_main_docum("c_kl_dt_2_kpp")
			,z_main_docum("c_kl_kt_1_1")
			,z_main_docum("c_kl_kt_1_2")
			,z_main_docum("c_kl_kt_2_1")
			,z_main_docum("c_kl_kt_2_inn")
			,z_main_docum("c_kl_kt_2_2")
			,z_main_docum("c_kl_kt_2_3")
			,z_main_docum("c_kl_kt_2_kpp")
			,z_main_docum("c_nazn")
			,z_main_docum("c_sum").cast("Decimal(23,5)").alias("c_sum")
			,z_main_docum("c_valuta")
			,z_main_docum("c_vid_doc")
			,z_main_docum("c_kod_nazn_pay")
			,z_main_docum("c_valuta_po")
			,z_main_docum("c_sum_po").cast("Decimal(23,5)").alias("c_sum_po")
			,z_main_docum("c_multicurr")
			,z_main_docum("c_sum_nt").cast("Decimal(23,5)").alias("c_sum_nt")
			,z_main_docum("c_type_mess")
			,z_main_docum("c_code_doc")
			,z_main_docum("c_num_dt")
			,z_main_docum("c_num_kt")
			,z_main_docum("c_filial")
			,z_main_docum("c_depart")
			,when(table2("id").isNotNull, table2("c_client_v")).otherwise(lit("-100")).alias("c_client_v")
		).where(table2("id").isNotNull || inn_table("inn").isNotNull)
		.union(
			z_main_docum
				.join(broadcast(table2), z_main_docum("c_acc_dt") === table2("id"), "left")
				.join(broadcast(inn_table), z_main_docum("C_KL_DT_2_INN") === inn_table("inn"), "left") //20180502 Ключко Исправил inner на left т.к. не попадали проводки
		.select(
			z_main_docum("id")
			,lit("1").alias("ktdt")
			,z_main_docum("class_id")
			,z_main_docum("state_id")
			,z_main_docum("c_acc_dt")
			,z_main_docum("c_acc_kt")
			// ,from_unixtime((z_main_docum("c_date_prov") / 1000).cast("Int")).alias("c_date_prov")
			,z_main_docum("c_date_prov")
			,z_main_docum("c_kl_dt_1_1")
			,z_main_docum("c_kl_dt_1_2")
			,z_main_docum("c_kl_dt_2_1")
			,z_main_docum("c_kl_dt_2_inn")
			,z_main_docum("c_kl_dt_2_2")
			,z_main_docum("c_kl_dt_2_3")
			,z_main_docum("c_kl_dt_2_kpp")
			,z_main_docum("c_kl_kt_1_1")
			,z_main_docum("c_kl_kt_1_2")
			,z_main_docum("c_kl_kt_2_1")
			,z_main_docum("c_kl_kt_2_inn")
			,z_main_docum("c_kl_kt_2_2")
			,z_main_docum("c_kl_kt_2_3")
			,z_main_docum("c_kl_kt_2_kpp")
			,z_main_docum("c_nazn")
			,z_main_docum("c_sum").cast("Decimal(23,5)").alias("c_sum")
			,z_main_docum("c_valuta")
			,z_main_docum("c_vid_doc")
			,z_main_docum("c_kod_nazn_pay")
			,z_main_docum("c_valuta_po")
			,z_main_docum("c_sum_po").cast("Decimal(23,5)").alias("c_sum_po")
			,z_main_docum("c_multicurr")
			,z_main_docum("c_sum_nt").cast("Decimal(23,5)").alias("c_sum_nt")
			,z_main_docum("c_type_mess")
			,z_main_docum("c_code_doc")
			,z_main_docum("c_num_dt")
			,z_main_docum("c_num_kt")
			,z_main_docum("c_filial")
			,z_main_docum("c_depart")
			,when(table2("id").isNotNull, table2("c_client_v")).otherwise(lit("-100")).alias("c_client_v")
		).where(table2("id").isNotNull || inn_table("inn").isNotNull)
	)


}
