package ru.sberbank.sdcb.k7m.core.pack

import scala.collection.mutable

class TableMeta(val name: String, val sourceSchema: String, val columns: String, val Where: Any, val Window: Any,
								val types: String, val raw_columns: String) {

	private val pref = sourceSchema match {
			case "internal_eks_ibs" | "core_internal_eks"					 => "eks"
			case "internal_crm_cb_siebel" | "core_internal_crm_kb" => "crm"
			case "internal_crm_fo_sber1" | "core_internal_fok"		 => "fok"
			case "external_integrum" | "custom_cb_akm_integrum"	|
					 "core_internal_integrum" | "cb_akm_integrum"	 		 => "int"
			case "external_pravo" | "custom_cb_akm_pravo" |
					 "cb_akm_pravo"					 													 => "prv"
			case "internal_bir_bir" 															 => "bir"
			case "core_internal_amrlirt" | "internal_amrlirt_anamod_sbrfsc_b0" |
					 "core_internal_amrlirt_lgd" | "internal_amrlirt_anamod_sbrflgd_b0" => "amr"
			case "internal_crm_rb_siebel" 												 => "cmr"
			case _ if sourceSchema.matches("internal_tsm.*")			 => "tsm"
			case "internal_mdm_mdm"																 => "mdm"
			case "internal_rdm" | "dwh_vd_rdm" 										 => "rdm"
			case "external_fns"																		 => "fns"
			case "ods_dm_c7m"																			 => "ods"
			case "internal_mmz_dm_antifraud_7m"                    => "mmz"
			case "external_pravobesp"                    => "rl"
			case _ 																								 => "sourceNotFound"
		}

	val prefix: String = pref + "_"

	val sourceFullName: String = sourceSchema + "." + name
	val nameWithPrefix: String = prefix + name

	def saveFullName(saveSchema: String): String = saveSchema + "." + nameWithPrefix
	def saveFullPath(savePath: String): String = savePath + nameWithPrefix

  def fieldsTypesMap(): mutable.Map[String, String] = {
    val typesSeparated = types.replaceAll("\\s", "").split(",")
    val columnsSeparated = raw_columns.replaceAll("\\s", "").split(",")

    val typesLength = typesSeparated.length
    val columnsLength = columnsSeparated.length

    if (typesLength != columnsLength && columnsLength != 1) {
      //TODO пока в s2t есть поля без типа throw new IllegalStateException(s"Ошибка в S2T файле для таблицы $name: не задан тип или наименование колонки!")
      return mutable.Map.empty[String, String]
    }

    if (columnsLength == 1 && columns == "*") {
      return mutable.Map.empty[String, String]
    }

    val result = mutable.Map[String, String]()

    for (elem <- columnsSeparated.zip(typesSeparated)) {
      result.put(elem._1, elem._2)
    }
    result
  }

}
