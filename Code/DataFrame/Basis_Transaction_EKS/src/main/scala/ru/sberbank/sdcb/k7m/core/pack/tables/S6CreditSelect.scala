package ru.sberbank.sdcb.k7m.core.pack.tables

import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Basis_KSB_Main, Config}

class S6CreditSelect(config: Config) extends Table(config: Config){

	val dashboardName: String = KreditSelectName
	val dashboardPath: String = KreditSelectPath

	import spark.implicits._

	private val table4 = spark.table(MainDocumJoinDictsName)

	val dataframe: DataFrame = table4.select(
		$"id",
		$"ktdt",
		$"C_KL_KT_1_1",
		$"C_KL_KT_2_INN")
		.where($"ktdt" === 1)

}
