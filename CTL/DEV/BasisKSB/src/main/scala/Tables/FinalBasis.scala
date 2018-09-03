package Tables

import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.DataFrame

class FinalBasis extends Table{
	val name: String = FinalBasisName

	private val innsec = spark.table(ClientFilterEndName)
	private val table4 = spark.table(MainDocumJoinDictsName)

	private val innimport010617_5a = table4.join(innsec,table4("c_kl_kt_1_1") === innsec("id"),"left")
		.select(
			table4("*"),
			coalesce(table4("c_kl_kt_2_INN"), innsec("c_inn")).alias("inn_sec"),
			innsec("c_name").alias("kl_2_org"))
		.where(table4("ktdt") === 1)

	private val innimport010617_5b = table4.join(innsec,table4("c_kl_dt_1_1") === innsec("id"),"left")
		.select(
			table4("*"),
			coalesce(table4("c_kl_dt_2_INN"), innsec("c_inn")).alias("inn_sec"),
			innsec("c_name").alias("kl_2_org"))
		.where(table4("ktdt") === 0)

	val dataframe: DataFrame = innimport010617_5a.union(innimport010617_5b)

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
		"c_type_mess_label string, " +
		"inn_sec string, " +
		"kl_2_org string"
}
