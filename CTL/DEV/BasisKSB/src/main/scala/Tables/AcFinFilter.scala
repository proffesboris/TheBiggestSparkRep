package Tables

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.DataFrame

class AcFinFilter extends Table{

	val name: String = AcFinFilterName

	private val table1 = spark.table(ClientFilterBegName)

	val dataframe: DataFrame = z_ac_fin.join(broadcast(table1), z_ac_fin("c_client_v") === table1("id"), "inner")
		.select(
			z_ac_fin("id"),
			z_ac_fin("c_client_v")
		)

	val SQLTableStructure: String =
		"id decimal(38, 12), " +
		"c_client_v decimal(38, 12)"
}
