package ru.sberbank.sdcb.k7m.core.pack.BEN_CRM.CRM_STG

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class CRM_STG_DOC(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(CRM_stg_doc_ShortName)
	val dashboardPath: String = genDashBoardPath(CRM_stg_doc_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT
			|      obj_id as contact_id
			|    , type
			|    , series
			|    , doc_number
			|    , registrator
			|    , issue_dt
			|    , birth_place
			|    , reg_code
			|    , end_date
			|    FROM $ZeroLayerSchema.crm_CX_REG_DOC crd
			|    WHERE lower(status) = 'действует'
		""".stripMargin)

}
