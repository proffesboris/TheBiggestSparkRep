package Tables

import org.apache.spark.sql.DataFrame

class DebitSelect extends Table {

	val name: String = DebitSelectName

	import spark.implicits._

	private val table4 = spark.table(MainDocumJoinDictsName)

	val dataframe: DataFrame = table4.select(
		$"id",
		$"ktdt",
		$"C_KL_DT_1_1",
		$"C_KL_DT_2_INN")
		.where($"ktdt" === 0)

	val SQLTableStructure: String =
		"id decimal(38,12), " +
		"ktdt string, " +
		"c_kl_dt_1_1 decimal(38,12), " +
		"c_kl_dt_2_inn string"
}
