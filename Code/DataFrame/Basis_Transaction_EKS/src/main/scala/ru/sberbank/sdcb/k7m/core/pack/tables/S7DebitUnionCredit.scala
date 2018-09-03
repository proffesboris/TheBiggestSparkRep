package ru.sberbank.sdcb.k7m.core.pack.tables

import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Basis_KSB_Main, Config}

class S7DebitUnionCredit(config: Config) extends Table(config: Config){

	val dashboardName: String = DebitUnionKreditName
	val dashboardPath: String = DebitUnionKreditPath

	import spark.implicits._

	private val dt = spark.table(DebitSelectName)
	private val kt = spark.table(KreditSelectName)

	private val pre_distclient_hive010617a = dt.select($"C_KL_DT_1_1").withColumnRenamed("c_kl_dt_1_1", "idclient")
	private val pre_distclient_hive010617b = kt.select($"C_KL_KT_1_1").withColumnRenamed("c_kl_kt_1_1", "idclient")
	private val pre_distclient_hive010617c = pre_distclient_hive010617a.union(pre_distclient_hive010617b)

	val dataframe: DataFrame = pre_distclient_hive010617c.select($"idclient").distinct

}
