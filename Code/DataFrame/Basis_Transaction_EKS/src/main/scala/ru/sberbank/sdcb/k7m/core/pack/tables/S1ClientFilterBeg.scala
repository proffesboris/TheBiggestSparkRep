package ru.sberbank.sdcb.k7m.core.pack.tables

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Basis_KSB_Main, Config}

class S1ClientFilterBeg(config: Config, inn_source: String) extends Table(config: Config) {

	val dashboardName: String= ClientFilterBegName
	val dashboardPath: String = ClientFilterBegPath

	import spark.implicits._

	private val innTable = inn_source match {
		case "basis_client" => spark.table(s"${config.aux}.basis_client").select($"org_inn_crm_num".alias("inn"))
		case "clu" =>  spark.table(s"${config.pa}.clu").select($"inn".alias("inn"))
	}

	val dataframe: DataFrame = z_client.join(broadcast(innTable), z_client("c_inn") === innTable("inn"), "inner")
		.select(
			$"id",
			$"C_INN"
		)

}
