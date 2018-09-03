package ru.sberbank.sdcb.k7m.core.pack.tran_misc

trait FieldNames {

	val keyFields: String = "id"

	val ktdtField: String = "ktdt"
	val classIdField: String = "class_id"
	val stateIdField: String = "state_id"

	val requiedFields: String =
		"""
			|c_acc_dt,
			|c_acc_kt,
			|c_date_prov,
			|c_kl_dt_2_1,
			|c_kl_dt_2_2,
			|c_kl_dt_2_inn,
			|c_kl_kt_2_1,
			|c_kl_kt_2_2,
			|c_kl_kt_2_inn,
			|c_nazn,
			|c_num_dt,
			|c_num_kt,
			|c_sum,
			|c_sum_nt,
			|c_sum_po,
			|c_type_mess,
			|c_valuta,
			|c_valuta_po,
			|c_vid_doc
		""".stripMargin

	val requiedFields2: String =
		"""
      |c_kl_dt_1_1,
      |c_kl_dt_1_2,
      |c_kl_dt_2_3,
      |c_kl_dt_2_kpp,
      |c_kl_kt_1_1,
      |c_kl_kt_1_2,
      |c_kl_kt_2_3,
      |c_kl_kt_2_kpp,
			|c_kod_nazn_pay,
			|c_multicurr,
			|c_code_doc,
			|c_filial,
			|c_depart,
			|c_client_v,
			|c_valuta_label,
			|c_valuta_po_label,
			|c_vid_doc_label,
			|c_kod_nazn_pay_label,
			|inn_st,
			|c_filial_label,
			|c_multicurr_label,
			|c_type_mess_label,
			|inn_sec,
			|kl_2_org
		""".stripMargin

	val hashFields: String = s"""coalesce(cast(${requiedFields.replace(",", " as string),''),';#'coalesce(cast(,")} as string),'')"""

	val hashFields2: String = requiedFields2.replace(",", ",';#',")

	val krasField: String = "predicted_value"

	val krasField2: String = "ypred"

	val hashField: String = "hash_sum"

	val dtField: String = "insert_dt"

}
