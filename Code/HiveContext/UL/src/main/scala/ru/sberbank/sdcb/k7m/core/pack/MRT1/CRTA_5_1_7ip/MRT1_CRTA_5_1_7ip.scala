package ru.sberbank.sdcb.k7m.core.pack.MRT1.CRTA_5_1_7ip

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DecimalType
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class MRT1_CRTA_5_1_7ip(config: Config) extends Table(config: Config){

	import spark.implicits._

	val dashboardName: String = genDashBoardName(MRT1_Crta_5_1_7ip_ShortName)
	val dashboardPath: String = genDashBoardPath(MRT1_Crta_5_1_7ip_ShortName)

	val MRT1_crta_5_1_8_Name = genDashBoardName(MRT1_Crta_5_1_8_ShortName)

	val table5_1_8 = spark.table(MRT1_crta_5_1_8_Name)

	val pre_dataframe = table5_1_8.where($"quantity" < 0.0001)

	val dataframe = pre_dataframe.select(
		$"inn1",
		$"inn1".as("inn2"),
		lit("5.1.7ip").as("criterion"),
		lit(100).as("confidence"),
		lit(1).cast(DecimalType(22, 5)).as("quantity"),
		lit(DATE).as("dt")
	).distinct

}
