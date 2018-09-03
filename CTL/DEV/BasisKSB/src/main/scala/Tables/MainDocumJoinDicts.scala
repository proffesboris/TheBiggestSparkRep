package Tables

import org.apache.spark.sql.functions.{broadcast, upper, when}
import org.apache.spark.sql.DataFrame

class MainDocumJoinDicts extends Table{

	val name: String = MainDocumJoinDictsName

	private val table1 = spark.table(ClientFilterBegName)
	private val table3 = spark.table(MainDocumFilterName)
	
	val dataframe: DataFrame = table3
		.join(broadcast(z_ft_money1), table3("c_valuta") === z_ft_money1("id") , "left")
		.join(broadcast(z_ft_money2), table3("c_valuta_po") === z_ft_money2("id") , "left")
		.join(broadcast(z_name_paydoc), table3("c_vid_doc") === z_name_paydoc("id"), "left")
		.join(broadcast(z_kod_n_pay), table3("c_kod_nazn_pay") === z_kod_n_pay("id"), "left")
		.join(broadcast(table1), table3("c_client_v") === table1("id"), "left")
		.join(broadcast(z_branch), table3("c_filial") === z_branch("id"), "left")
		.join(broadcast(z_sbrf_type_mess), table3("c_type_mess") === z_sbrf_type_mess("id"), "left")
		.select(
			table3("*")
			,z_ft_money1("C_CUR_ATTR_CUR_P_2UNIT").alias("C_VALUTA_LABEL")
			,z_ft_money2("C_CUR_ATTR_CUR_P_2UNIT").alias("c_valuta_po_LABEL")
			,z_name_paydoc("C_NAME").alias("c_vid_doc_LABEL")
			,z_kod_n_pay("C_MEMO_NAZN").alias("c_kod_nazn_pay_LABEL")
			,when(table1("id").isNotNull, table1("c_inn"))
				.when(table3("ktdt") === 0, table3("C_KL_KT_2_INN"))
				.when(table3("ktdt") === 1, table3("C_KL_DT_2_INN"))
		  	.alias("inn_st")
			,z_branch("c_shortlabel").alias("c_filial_label")
			,when(table3("c_multicurr") === "1920418", "Область покрытия плательщика")
				.when(table3("c_multicurr") === "1920419", "Область покрытия  получателя")
				.when(table3("c_multicurr") === "1920420", "Мультивалютность")
				.when(table3("c_multicurr") === "1920421", "Область национального покрытия")
				.otherwise(null)
				.alias("c_multicurr_label")
			,z_sbrf_type_mess("c_name").alias("c_type_mess_label")
		).where(upper(table3("state_id"))==="PROV" && upper(table3("class_id")) === "MAIN_DOCUM")

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
		"c_client_v string, " +
		"c_valuta_label string, " +
		"c_valuta_po_label string, " +
		"c_vid_doc_label string, " +
		"c_kod_nazn_pay_label string, " +
		"inn_st string, " +
		"c_filial_label string, " +
		"c_multicurr_label string, " +
		"c_type_mess_label string"
	
}
