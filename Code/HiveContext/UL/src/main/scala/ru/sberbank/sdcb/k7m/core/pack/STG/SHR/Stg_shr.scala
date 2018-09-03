package ru.sberbank.sdcb.k7m.core.pack.STG.SHR

import org.apache.spark.sql.DataFrame
import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class Stg_shr(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(STG_shr_ShortName)
	val dashboardPath: String = genDashBoardPath(STG_shr_ShortName)

	private val Stg_shr_T1_name = genDashBoardName(STG_shr_T1_ShortName)

	val dataframe: DataFrame = spark.sql(s"""
			select a.*
			from
			$Stg_shr_T1_name a
			left join
			(
					select distinct sh_ul_inn
					from $Stg_shr_T1_name
					group by sh_ul_inn
					having sum(sh_share_prcnt_cnt) -1 > 0.0001
			) b
			on a.sh_ul_inn = b.sh_ul_inn
			where b.sh_ul_inn is null

			""")

}
