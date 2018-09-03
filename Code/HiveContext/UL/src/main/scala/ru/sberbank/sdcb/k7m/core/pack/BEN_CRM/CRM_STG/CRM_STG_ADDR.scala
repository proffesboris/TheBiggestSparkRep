package ru.sberbank.sdcb.k7m.core.pack.BEN_CRM.CRM_STG

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class CRM_STG_ADDR(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(CRM_stg_addr_ShortName)
	val dashboardPath: String = genDashBoardPath(CRM_stg_addr_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT   accnt_id
			|       , contact_id
			|  , max(case when lower(ca.addr_type_cd)='юридический' then a.addr else null end) as jur_addr
			|  , max(case when lower(ca.addr_type_cd)='фактический' then a.addr else null end) as fact_addr
			|FROM
			| (
			|         SELECT addr_per_id,  accnt_id, contact_id, addr_type_cd
			|         FROM $ZeroLayerSchema.crm_s_con_addr
			|         WHERE lower(active_flg) = 'y'
			|         AND lower(addr_type_cd) in ('юридический','фактический')
			|) ca
			|INNER JOIN (SELECT row_id as addr_id , addr  FROM $ZeroLayerSchema.crm_s_addr_per) a
			|ON (a.addr_id = ca.addr_per_id)
			|GROUP BY accnt_id, contact_id
		""".stripMargin)

}
