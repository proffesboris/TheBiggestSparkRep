package My_Utils

trait Variables {

	val SaveSchema = "custom_cb_preapproval_mdb"
	val InnTableSchema = "custom_cb_preapproval_mdb"
	val SourceSchema = "internal_eks_ibs"

	/** Table of source Inns received froam business*/
	val InnTableName: String = InnTableSchema + "." + "COUNTERP"

	/** Names of tables that are built inside this applications*/
	val ClientFilterBegName: String = SaveSchema + "." + "innimport190417_t17_1"
	val AcFinFilterName: String = SaveSchema + "." + "innimport190417_t17_2"
	val MainDocumFilterName: String = SaveSchema + "." + "innimport190417_t17_3"
	val MainDocumJoinDictsName: String = SaveSchema + "." + "innimport190417_t17_4"

	val DebitSelectName: String = SaveSchema + "." + "dt1_hive010617"
	val KreditSelectName: String = SaveSchema + "." + "kt1_hive010617"

	val DebitUnionKreditName: String = SaveSchema + "." + "distclient_hive010617"
	val ClientFilterEndName: String = SaveSchema + "." + "innsec_hive010617"

	val FinalBasisName: String = SaveSchema + "." + "smart_src"

}
