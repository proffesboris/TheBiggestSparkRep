package Tables

import org.apache.spark.sql.DataFrame

class KreditSelect extends Table {

	val name: String = KreditSelectName

	import spark.implicits._

	private val table4 = spark.table(MainDocumJoinDictsName)

	val dataframe: DataFrame = table4.select(
		$"id",
		$"ktdt",
		$"C_KL_KT_1_1",
		$"C_KL_KT_2_INN")
		.where($"ktdt" === 1)

	val SQLTableStructure: String =
		"id decimal(38,12), " +
		"ktdt string, " +
		"c_kl_kt_1_1 decimal(38,12), " +
		"c_kl_kt_2_inn string"
}
