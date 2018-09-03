package ru.sberbank.sdcb.k7m.core.pack.BEN_CRM.CRM_STG

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class CRM_STG_CUST(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(CRM_stg_cust_ShortName)
	val dashboardPath: String = genDashBoardPath(CRM_stg_cust_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT
			| org.row_id                            as crm_id         ---ID организации
			|,org.pr_postn_id                       as pos_vko_id     ---ID позиции ВКО
			|,org.x_osb_div_id                      as pos_terr_id    ---Ссылка на территориальное подразделение в оргструктуре, к которому относится данная позиция
			|,org.bu_id                             as id_tb_holf    ---ID закрепления (ЦА/ТБ основной)
			|,org_x.sbrf_kpp                        as kpp           ---КПП
			|,org_x.sbrf_inn                        as inn
			|,org_x.attrib_39                       as opf
			|,org_x.attrib_51                       as okato       ---ОКАТО в соответствии с юридическим адресом
			|,org.name                              as full_name    ---Полное наименование
			|,org_x.attrib_07                       as resident_status --резидентство
			|FROM  $ZeroLayerSchema.crm_S_ORG_EXT            org
			|LEFT JOIN $ZeroLayerSchema.crm_S_ORG_EXT_X      org_x
			|ON org.ROW_ID = org_x.PAR_ROW_ID
			|WHERE lower(org.INT_ORG_FLG)='n'
			|AND  (lower(org.OU_TYPE_CD) not in ('территория'))
			|    and (org_x.sbrf_inn is not null)
			|    and (org_x.sbrf_inn not like '0000000%')
			|    and (org_x.sbrf_inn <> '' )
		""".stripMargin)



}
