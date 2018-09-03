package ru.sberbank.sdcb.k7m.core.pack.tables

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Basis_KSB_Main, Config}

class S8ClientFilterEnd(config: Config) extends Table(config: Config){

	val dashboardName: String = ClientFilterEndName
	val dashboardPath: String = ClientFilterEndPath

	import spark.implicits._

	private val distclient = spark.table(DebitUnionKreditName)

	private val pre_innsec = distclient.select("idclient").where($"idclient".isNotNull)
	private val pre_innsec_hive010617 = z_client.join(broadcast(pre_innsec),z_client("id") === pre_innsec("idclient"),"inner").select(z_client("*"))

	val dataframe: DataFrame = pre_innsec_hive010617.select("*")

}
