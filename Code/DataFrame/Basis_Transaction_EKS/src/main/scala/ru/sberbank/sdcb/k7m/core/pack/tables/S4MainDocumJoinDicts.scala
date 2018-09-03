package ru.sberbank.sdcb.k7m.core.pack.tables

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ru.sberbank.sdcb.k7m.core.pack.{Basis_KSB_Main, Config}

class S4MainDocumJoinDicts(config: Config) extends Table(config: Config){

	val dashboardName: String= MainDocumJoinDictsName
	val dashboardPath: String = MainDocumJoinDictsPath

	private val table1 = spark.table(ClientFilterBegName)
	private val table3 = spark.table(MainDocumFilterName)
	
	val dataframe: DataFrame = table3
		.join(broadcast(z_ft_money1), table3("c_valuta") === col("z_ft_money1.id"), "left")
		.join(broadcast(z_ft_money2), table3("c_valuta_po") === col("z_ft_money2.id"), "left")
		.join(broadcast(z_name_paydoc), table3("c_vid_doc") === z_name_paydoc("id"), "left")
		.join(broadcast(z_kod_n_pay), table3("c_kod_nazn_pay") === z_kod_n_pay("id"), "left")
		.join(broadcast(table1), table3("c_client_v") === table1("id"), "left")
		.join(broadcast(z_branch), table3("c_filial") === z_branch("id"), "left")
		.join(broadcast(z_sbrf_type_mess), table3("c_type_mess") === z_sbrf_type_mess("id"), "left")
		.select(
			table3("*")
			,col("z_ft_money1.C_CUR_ATTR_CUR_P_2UNIT").alias("C_VALUTA_LABEL")
			,col("z_ft_money2.C_CUR_ATTR_CUR_P_2UNIT").alias("c_valuta_po_LABEL")
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

}
