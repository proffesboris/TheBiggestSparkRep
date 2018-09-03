package Tables

import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.DataFrame

class ClientFilterBeg extends Table{

	val name: String = ClientFilterBegName

	import spark.implicits._

	private val innTable = spark.table(InnTableName)

	val dataframe: DataFrame = z_client.join(broadcast(innTable), z_client("c_inn") === innTable("inn"), "inner")
		.select(
			$"id",
			$"C_INN"
		)

	val SQLTableStructure: String =
		"id decimal(38, 12), " +
		"c_inn string"


}
