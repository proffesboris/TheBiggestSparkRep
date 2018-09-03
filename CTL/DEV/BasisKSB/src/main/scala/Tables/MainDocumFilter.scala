package Tables

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


class MainDocumFilter extends Table{

	val name: String = MainDocumFilterName

	private val inn_table = spark.table(InnTableName)
	private val table2 = spark.table(AcFinFilterName)

	val dataframe: DataFrame = z_main_docum
		.join(broadcast(table2), z_main_docum("c_acc_kt") === table2("id"), "left_outer")
		.join(broadcast(inn_table), z_main_docum("C_KL_KT_2_INN") === inn_table("inn"), "left_outer")
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
				.join(broadcast(table2), z_main_docum("c_acc_dt") === table2("id"), "left_outer")
				.join(broadcast(inn_table), z_main_docum("C_KL_DT_2_INN") === inn_table("inn"), "left_outer")
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

	val SQLTableStructure: String =
		"id decimal(38,12), " +
		"ktdt string, " +
		"class_id string, " +
		"state_id string, " +
		"c_acc_dt decimal(38,12), " +
		"c_acc_kt decimal(38,12), " +
		"c_date_prov timestamp, " +
		"c_kl_dt_1_1 decimal(38,12), " +
		"c_kl_dt_1_2 decimal(38,12), " +
		"c_kl_dt_2_1 string, " +
		"c_kl_dt_2_inn string, " +
		"c_kl_dt_2_2 string, " +
		"c_kl_dt_2_3 decimal(38,12), " +
		"c_kl_dt_2_kpp string, " +
		"c_kl_kt_1_1 decimal(38,12), " +
		"c_kl_kt_1_2 decimal(38,12), " +
		"c_kl_kt_2_1 string, " +
		"c_kl_kt_2_inn string, " +
		"c_kl_kt_2_2 string, " +
		"c_kl_kt_2_3 decimal(38,12), " +
		"c_kl_kt_2_kpp string, " +
		"c_nazn string, " +
		"c_sum decimal(23,5), " +
		"c_valuta decimal(38,12), " +
		"c_vid_doc decimal(38,12), " +
		"c_kod_nazn_pay decimal(38,12), " +
		"c_valuta_po decimal(38,12), " +
		"c_sum_po decimal(23,5), " +
		"c_multicurr decimal(38,12), " +
		"c_sum_nt decimal(23,5), " +
		"c_type_mess decimal(38,12), " +
		"c_code_doc decimal(38,12), " +
		"c_num_dt string, " +
		"c_num_kt string, " +
		"c_filial decimal(38,12), " +
		"c_depart decimal(38,12), " +
		"c_client_v string"

}
