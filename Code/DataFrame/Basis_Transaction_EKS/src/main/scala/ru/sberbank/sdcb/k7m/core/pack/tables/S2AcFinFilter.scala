package ru.sberbank.sdcb.k7m.core.pack.tables

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Basis_KSB_Main, Config}

class S2AcFinFilter(config: Config) extends Table(config: Config){

	val dashboardName: String = AcFinFilterName
	val dashboardPath: String = AcFinFilterPath

	private val table1 = spark.table(ClientFilterBegName)

	val dataframe: DataFrame = z_ac_fin.join(broadcast(table1), z_ac_fin("c_client_v") === table1("id"), "inner")
		.select(
			z_ac_fin("id"),
			z_ac_fin("c_client_v")
		)

}
