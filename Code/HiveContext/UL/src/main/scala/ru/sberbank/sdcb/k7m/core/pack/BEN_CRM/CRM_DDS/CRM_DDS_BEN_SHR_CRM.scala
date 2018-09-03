package ru.sberbank.sdcb.k7m.core.pack.BEN_CRM.CRM_DDS

import ru.sberbank.sdcb.k7m.core.pack.{Config, Main, Table}

class CRM_DDS_BEN_SHR_CRM(config: Config) extends Table(config: Config){

	val dashboardName: String = genDashBoardName(DDS_BEN_SHR_CRM_T1_ShortName)
	val dashboardPath: String = genDashBoardPath(DDS_BEN_SHR_CRM_T1_ShortName)

	val stg_cust_Name: String = genDashBoardName(CRM_stg_cust_ShortName)
	val stg_addr_Name: String = genDashBoardName(CRM_stg_addr_ShortName)
	val stg_doc_Name: String  = genDashBoardName(CRM_stg_doc_ShortName)

	val dataframe = spark.sql(
		s"""
			|SELECT
			|          cust1.crm_id --
			|        , cust1.inn
			|        , cust1.kpp
			|        , cust1.full_name
			|        , cust2.crm_id as benorg_id--
			|        , cust2.inn    as benorg_inn
			|        , cust2.kpp    as benorg_kpp
			|        , cust2.full_name as benorg_full_name
			|        , cust2.opf    as benorg_opf
			|        , addrj.jur_addr  as org_jur_addr
			|        , addrj.fact_addr as org_fact_addr
			|        , con.row_id   as fz_ben
			|        , con.fst_name
			|        , con.mid_name
			|        , con.last_name
			|        , con.birth_dt
			|        , con.sex_mf
			|        , con.citizenship_cd
			|        , con.home_ph_num
			|        , con.work_ph_num
			|        , con.cell_ph_num
			|        , con.email_addr
			|        , addrc.jur_addr as fz_jur_addr
			|        , addrc.fact_addr as fz_fact_addr
			|        , rd.type    ---- тип дока
			|      , rd.series    --  Серия
			|      , rd.doc_number    ---Место рождения
			|      , rd.registrator   ----Кем выдан
			|      , rd.issue_dt   ---Дата выдачи
			|      , rd.birth_place   ---Место рождения
			|      , rd.reg_code  ---Код подразделения
			|      , rd.end_date ----Планируемая дата окончания срока действия
			|       , cust1.resident_status
			|        , 'ben_crm'            as crit
			|      ,  cast(ben.shares as decimal(22,5)) / 100            as quantity
			|      , 100                    as link_prob ---100%
			|
			|FROM $stg_cust_Name cust1
			|INNER JOIN $ZeroLayerSchema.crm_cx_party_benef ben
			|    ON  ben.account_id  = cust1.crm_id
			|LEFT JOIN $ZeroLayerSchema.crm_s_contact con
			|    ON  ben.benef_id    = con.row_id
			|LEFT JOIN $stg_addr_Name addrc
			|    ON  addrc.contact_id  = con.row_id
			|LEFT JOIN $stg_cust_Name cust2
			|    ON  ben.benef_id    = cust2.crm_id
			|LEFT JOIN $stg_addr_Name   addrj
			|    ON  addrj.accnt_id  = cust2.crm_id
			|LEFT JOIN $stg_doc_Name rd
			|    ON  con.row_id      = rd.contact_id
		""".stripMargin)
}
