package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class ClfFpersMdmKiClass3 (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

	import spark.implicits._

	val DevSchema = config.aux

	val Node1t_team_k7m_aux_d_clf_fpers_mdm_kiIN = s"${DevSchema}.tmp_clf_raw_keys"
	val Node2t_team_k7m_aux_d_clf_fpers_mdm_kiIN = s"${DevSchema}.tmp_clf_mdm_dedubl"
	val  Nodet_team_k7m_aux_d_clf_fpers_mdm_kiOUT = s"${DevSchema}.clf_fpers_mdm_ki"
	val dashboardPath = s"${config.auxPath}clf_fpers_mdm_ki"


	override val dashboardName: String = Nodet_team_k7m_aux_d_clf_fpers_mdm_kiOUT //витрина
	override def processName: String = "CLF"

	private val tmp_clf_raw_keys = spark.table(Node1t_team_k7m_aux_d_clf_fpers_mdm_kiIN)
	private val tmp_clf_mdm_dedubl = spark.table(Node2t_team_k7m_aux_d_clf_fpers_mdm_kiIN)

	private val tmp_clf_mdm_dedubl_filtered = tmp_clf_mdm_dedubl.where($"rn_inn" === 1 || $"rn_id" === 1 || $"rn_tel" === 1 || $"rn_inn_only" === 1)

	private val tmp_clf_mdm_dedubl_enriched = tmp_clf_mdm_dedubl_filtered.select(
		$"mdm_id",
		$"mdm_active_flag",
		$"full_name",
		$"clf_l_name",
		$"clf_f_name",
		$"clf_m_name",
		$"status_active_dead",
		$"id_series_num",
		$"id_date",
		$"identifier_desc",
		$"issue_location",
		$"ID_SERIES",
		$"ID_NUM",
		$"birth_date",
		$"birth_place",
		$"id_end_date",
		$"gender_tp_code",
		$"citizenship",
		$"inn",
		$"adr_reg",
		$"adr_reg_postal_code",
		$"adr_reg_cntry_name",
		$"adr_reg_region",
		$"adr_reg_city_name",
		$"adr_reg_area",
		$"adr_reg_settlement",
		$"adr_reg_street_name",
		$"adr_reg_house_number",
		$"adr_reg_korpus_number",
		$"adr_reg_building_number",
		$"adr_reg_residence_num",
		$"adr_reg_kladr",
		$"adr_reg_kladr_1",
		$"adr_reg_kladr_2",
		$"adr_reg_kladr_3",
		$"adr_reg_kladr_4",
		$"adr_reg_kladr_5",
		$"adr_reg_kladr_6",
		$"adr_fact",
		$"adr_fact_postal_code",
		$"adr_fact_cntry_name",
		$"adr_fact_region",
		$"adr_fact_city_name",
		$"adr_fact_area",
		$"adr_fact_settlement",
		$"adr_fact_street_name",
		$"adr_fact_house_number",
		$"adr_fact_korpus_number",
		$"adr_fact_building_number",
		$"adr_fact_residence_num",
		$"adr_fact_kladr",
		$"adr_fact_kladr_1",
		$"adr_fact_kladr_2",
		$"adr_fact_kladr_3",
		$"adr_fact_kladr_4",
		$"adr_fact_kladr_5",
		$"adr_fact_kladr_6",
		$"tel_home",
		$"tel_mob",
		$"email",
		$"udbo_open_dt",
		$"udbo_active_flag",
		$"udbo_agreement_num",
		$"snils",
		$"systems_count",
		$"last_update_dt",
		$"full_name_clear",
		$"id_series_num_clear",
		$"inn_clear",
		$"birth_date_clear",
		$"tel_mob_clear",
		$"rn_inn",
		$"rn_id",
		$"rn_tel",
		$"rn_inn_only"
	).withColumn(
		"m1", when($"full_name_clear".isNotNull && $"id_series_num_clear".isNotNull && $"birth_date_clear".isNotNull,
			concat($"full_name_clear", lit("_"), $"id_series_num_clear",lit("_"), $"birth_date_clear")).otherwise(lit(null))
	).withColumn(
		"m2", when($"full_name_clear".isNotNull && $"birth_date_clear".isNotNull && $"tel_mob_clear".isNotNull,
			concat($"full_name_clear", lit("_"), $"birth_date_clear",lit("_"), $"tel_mob_clear")).otherwise(lit(null))
	).withColumn(
		"m3", when($"full_name_clear".isNotNull && $"inn_clear".isNotNull,
			concat($"full_name_clear", lit("_"), coalesce($"inn_clear", lit("-1")))).otherwise(lit(null))
	).withColumn(
		"m4", when($"inn_clear".isNotNull, $"inn_clear").otherwise(lit(null))
	)
	private val tmp_clf_raw_keys_enriched = tmp_clf_raw_keys.select(
		$"id" ,
		$"position",
		$"crm_id",
		$"eks_id",
		$"inn",
		$"fio",
		$"birth_dt",
		$"doc_type",
		$"doc_ser",
		$"doc_num",
		$"doc_date",
		$"cell_ph_num",
		$"crit",
		$"k1",
		$"k2",
		$"k3",
		$"k4",
		$"keys_fio",
		$"keys_inn",
		$"keys_doc_ser",
		$"keys_birth_dt",
		$"keys_cell_ph_num"
	).withColumn(
		"position_eks", when($"id".like("EK%"),$"position").otherwise(lit(null))
	).withColumn(
		"position_ul", when($"id".like("UL%"),$"position").otherwise(lit(null)))
		.withColumn(
		  "doc_ser_eks", when($"id".like("EK%"),$"doc_ser").otherwise(lit(null)))
		.withColumn(
			"doc_num_eks", when($"id".like("EK%"),$"doc_num").otherwise(lit(null)))
		.withColumn(
			"doc_date_eks", when($"id".like("EK%"),$"doc_date").otherwise(lit(null)))
//Если ИНН не найден в МДМ, то тащим его из источника, разделяя по конкретным источникам
		.withColumn(
			"inn_eks", when($"id".like("EK%"),$"keys_inn").otherwise(lit(null)))
		.withColumn(
			"inn_ul",  when($"id".like("UL%"),$"keys_inn").otherwise(lit(null)))
		.withColumn(
			"inn_ex",  when($"id".like("EX%"),$"keys_inn").otherwise(lit(null)))
		.withColumn(
			"inn_gs",  when($"id".like("GS%"),$"keys_inn").otherwise(lit(null)))

	private val tmp_clf_raw_keys_filtered = tmp_clf_raw_keys_enriched.where(length(trim(regexp_replace($"keys_inn","[^0-9]",""))) === 12 && ! $"keys_inn".isin("111111111111","100000000000","000000000000"))

	//private val union_part1 = tmp_clf_mdm_dedubl_enriched.join(broadcast(tmp_clf_raw_keys_filtered), tmp_clf_mdm_dedubl_enriched("inn_clear") === tmp_clf_raw_keys_filtered("keys_inn"), "left")
	private val union_part1 = tmp_clf_mdm_dedubl_enriched.join(broadcast(tmp_clf_raw_keys_filtered), tmp_clf_mdm_dedubl_enriched("m1") === tmp_clf_raw_keys_filtered("k1"))
		.select(
			 tmp_clf_raw_keys_filtered("id")
			,tmp_clf_raw_keys_filtered("position_eks")
			,tmp_clf_raw_keys_filtered("position_ul")
			,tmp_clf_raw_keys_filtered("doc_ser_eks")
			,tmp_clf_raw_keys_filtered("doc_num_eks")
			,tmp_clf_raw_keys_filtered("doc_date_eks")
			,tmp_clf_raw_keys_filtered("crm_id")
			,tmp_clf_raw_keys_filtered("inn_eks")
			,tmp_clf_raw_keys_filtered("inn_ul")
			,tmp_clf_raw_keys_filtered("inn_ex")
			,tmp_clf_raw_keys_filtered("inn_gs")
			,tmp_clf_mdm_dedubl_enriched("mdm_id")
			,tmp_clf_mdm_dedubl_enriched("mdm_active_flag")
			,tmp_clf_mdm_dedubl_enriched("full_name")
			,tmp_clf_mdm_dedubl_enriched("clf_l_name")
			,tmp_clf_mdm_dedubl_enriched("clf_f_name")
			,tmp_clf_mdm_dedubl_enriched("clf_m_name")
			,tmp_clf_mdm_dedubl_enriched("status_active_dead")
			,tmp_clf_mdm_dedubl_enriched("id_series_num")
			,tmp_clf_mdm_dedubl_enriched("id_date")
			,tmp_clf_mdm_dedubl_enriched("identifier_desc")
			,tmp_clf_mdm_dedubl_enriched("issue_location")
			,tmp_clf_mdm_dedubl_enriched("ID_SERIES")
			,tmp_clf_mdm_dedubl_enriched("ID_NUM")
			,tmp_clf_mdm_dedubl_enriched("birth_date")
			,tmp_clf_mdm_dedubl_enriched("birth_place")
			,tmp_clf_mdm_dedubl_enriched("id_end_date")
			,tmp_clf_mdm_dedubl_enriched("gender_tp_code")
			,tmp_clf_mdm_dedubl_enriched("citizenship")
			,tmp_clf_mdm_dedubl_enriched("inn")
			,tmp_clf_mdm_dedubl_enriched("adr_reg")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_postal_code")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_cntry_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_region")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_city_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_area")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_settlement")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_street_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_house_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_korpus_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_building_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_residence_num")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_1")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_2")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_3")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_4")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_5")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_6")
			,tmp_clf_mdm_dedubl_enriched("adr_fact")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_postal_code")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_cntry_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_region")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_city_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_area")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_settlement")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_street_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_house_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_korpus_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_building_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_residence_num")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_1")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_2")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_3")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_4")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_5")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_6")
			,tmp_clf_mdm_dedubl_enriched("tel_home")
			,tmp_clf_mdm_dedubl_enriched("tel_mob")
			,tmp_clf_mdm_dedubl_enriched("email")
			,tmp_clf_mdm_dedubl_enriched("udbo_open_dt")
			,tmp_clf_mdm_dedubl_enriched("udbo_active_flag")
			,tmp_clf_mdm_dedubl_enriched("udbo_agreement_num")
			,tmp_clf_mdm_dedubl_enriched("snils")
			,tmp_clf_mdm_dedubl_enriched("systems_count")
			,tmp_clf_mdm_dedubl_enriched("last_update_dt")
			,tmp_clf_mdm_dedubl_enriched("full_name_clear")
			,tmp_clf_mdm_dedubl_enriched("id_series_num_clear")
			,tmp_clf_mdm_dedubl_enriched("inn_clear")
			,tmp_clf_mdm_dedubl_enriched("birth_date_clear")
			,tmp_clf_mdm_dedubl_enriched("tel_mob_clear")
			,tmp_clf_mdm_dedubl_enriched("rn_inn")
			,tmp_clf_mdm_dedubl_enriched("rn_id")
			,tmp_clf_mdm_dedubl_enriched("rn_tel")
			,tmp_clf_mdm_dedubl_enriched("rn_inn_only")
			,tmp_clf_raw_keys_filtered("k1")
			,tmp_clf_raw_keys_filtered("k2")
			,tmp_clf_raw_keys_filtered("k3")
			,tmp_clf_raw_keys_filtered("k4")
			,tmp_clf_mdm_dedubl_enriched("m1")
			,tmp_clf_mdm_dedubl_enriched("m2")
			,tmp_clf_mdm_dedubl_enriched("m3")
			,tmp_clf_mdm_dedubl_enriched("m4")
			,lit(1).as("mkj")
		)

//private val union_part2 = tmp_clf_mdm_dedubl_enriched.join(broadcast(tmp_clf_raw_keys_enriched), tmp_clf_mdm_dedubl_enriched("full_name_clear") === tmp_clf_raw_keys_enriched("keys_fio"), "left")
private val union_part2 = tmp_clf_mdm_dedubl_enriched.join(broadcast(tmp_clf_raw_keys_filtered), tmp_clf_mdm_dedubl_enriched("m2") === tmp_clf_raw_keys_filtered("k2"))
	.select(
			 tmp_clf_raw_keys_filtered("id")
			,tmp_clf_raw_keys_filtered("position_eks")
			,tmp_clf_raw_keys_filtered("position_ul")
			,tmp_clf_raw_keys_filtered("doc_ser_eks")
			,tmp_clf_raw_keys_filtered("doc_num_eks")
			,tmp_clf_raw_keys_filtered("doc_date_eks")
			,tmp_clf_raw_keys_filtered("crm_id")
//			,lit(null).as("inn_eks")
//			,lit(null).as("inn_ul")
//			,lit(null).as("inn_ex")
//			,lit(null).as("inn_gs")
		  ,tmp_clf_raw_keys_filtered("inn_eks")
		  ,tmp_clf_raw_keys_filtered("inn_ul")
		  ,tmp_clf_raw_keys_filtered("inn_ex")
		  ,tmp_clf_raw_keys_filtered("inn_gs")
			,tmp_clf_mdm_dedubl_enriched("mdm_id")
			,tmp_clf_mdm_dedubl_enriched("mdm_active_flag")
			,tmp_clf_mdm_dedubl_enriched("full_name")
			,tmp_clf_mdm_dedubl_enriched("clf_l_name")
			,tmp_clf_mdm_dedubl_enriched("clf_f_name")
			,tmp_clf_mdm_dedubl_enriched("clf_m_name")
			,tmp_clf_mdm_dedubl_enriched("status_active_dead")
			,tmp_clf_mdm_dedubl_enriched("id_series_num")
			,tmp_clf_mdm_dedubl_enriched("id_date")
			,tmp_clf_mdm_dedubl_enriched("identifier_desc")
			,tmp_clf_mdm_dedubl_enriched("issue_location")
			,tmp_clf_mdm_dedubl_enriched("ID_SERIES")
			,tmp_clf_mdm_dedubl_enriched("ID_NUM")
			,tmp_clf_mdm_dedubl_enriched("birth_date")
			,tmp_clf_mdm_dedubl_enriched("birth_place")
			,tmp_clf_mdm_dedubl_enriched("id_end_date")
			,tmp_clf_mdm_dedubl_enriched("gender_tp_code")
			,tmp_clf_mdm_dedubl_enriched("citizenship")
			,tmp_clf_mdm_dedubl_enriched("inn")
			,tmp_clf_mdm_dedubl_enriched("adr_reg")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_postal_code")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_cntry_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_region")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_city_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_area")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_settlement")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_street_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_house_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_korpus_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_building_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_residence_num")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_1")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_2")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_3")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_4")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_5")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_6")
			,tmp_clf_mdm_dedubl_enriched("adr_fact")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_postal_code")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_cntry_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_region")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_city_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_area")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_settlement")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_street_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_house_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_korpus_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_building_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_residence_num")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_1")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_2")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_3")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_4")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_5")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_6")
			,tmp_clf_mdm_dedubl_enriched("tel_home")
			,tmp_clf_mdm_dedubl_enriched("tel_mob")
			,tmp_clf_mdm_dedubl_enriched("email")
			,tmp_clf_mdm_dedubl_enriched("udbo_open_dt")
			,tmp_clf_mdm_dedubl_enriched("udbo_active_flag")
			,tmp_clf_mdm_dedubl_enriched("udbo_agreement_num")
			,tmp_clf_mdm_dedubl_enriched("snils")
			,tmp_clf_mdm_dedubl_enriched("systems_count")
			,tmp_clf_mdm_dedubl_enriched("last_update_dt")
			,tmp_clf_mdm_dedubl_enriched("full_name_clear")
			,tmp_clf_mdm_dedubl_enriched("id_series_num_clear")
			,tmp_clf_mdm_dedubl_enriched("inn_clear")
			,tmp_clf_mdm_dedubl_enriched("birth_date_clear")
			,tmp_clf_mdm_dedubl_enriched("tel_mob_clear")
			,tmp_clf_mdm_dedubl_enriched("rn_inn")
			,tmp_clf_mdm_dedubl_enriched("rn_id")
			,tmp_clf_mdm_dedubl_enriched("rn_tel")
			,tmp_clf_mdm_dedubl_enriched("rn_inn_only")
			,tmp_clf_raw_keys_filtered("k1")
			,tmp_clf_raw_keys_filtered("k2")
			,tmp_clf_raw_keys_filtered("k3")
			,tmp_clf_raw_keys_filtered("k4")
			,tmp_clf_mdm_dedubl_enriched("m1")
			,tmp_clf_mdm_dedubl_enriched("m2")
			,tmp_clf_mdm_dedubl_enriched("m3")
			,tmp_clf_mdm_dedubl_enriched("m4")
		  ,lit(2).as("mkj")
		)

	private val union_part3 = tmp_clf_mdm_dedubl_enriched.join(broadcast(tmp_clf_raw_keys_filtered), tmp_clf_mdm_dedubl_enriched("m3") === tmp_clf_raw_keys_filtered("k3"))
		.select(
			 tmp_clf_raw_keys_filtered("id")
			,tmp_clf_raw_keys_filtered("position_eks")
			,tmp_clf_raw_keys_filtered("position_ul")
			,tmp_clf_raw_keys_filtered("doc_ser_eks")
			,tmp_clf_raw_keys_filtered("doc_num_eks")
			,tmp_clf_raw_keys_filtered("doc_date_eks")
			,tmp_clf_raw_keys_filtered("crm_id")
			,tmp_clf_raw_keys_filtered("inn_eks")
			,tmp_clf_raw_keys_filtered("inn_ul")
			,tmp_clf_raw_keys_filtered("inn_ex")
			,tmp_clf_raw_keys_filtered("inn_gs")
			,tmp_clf_mdm_dedubl_enriched("mdm_id")
			,tmp_clf_mdm_dedubl_enriched("mdm_active_flag")
			,tmp_clf_mdm_dedubl_enriched("full_name")
			,tmp_clf_mdm_dedubl_enriched("clf_l_name")
			,tmp_clf_mdm_dedubl_enriched("clf_f_name")
			,tmp_clf_mdm_dedubl_enriched("clf_m_name")
			,tmp_clf_mdm_dedubl_enriched("status_active_dead")
			,tmp_clf_mdm_dedubl_enriched("id_series_num")
			,tmp_clf_mdm_dedubl_enriched("id_date")
			,tmp_clf_mdm_dedubl_enriched("identifier_desc")
			,tmp_clf_mdm_dedubl_enriched("issue_location")
			,tmp_clf_mdm_dedubl_enriched("ID_SERIES")
			,tmp_clf_mdm_dedubl_enriched("ID_NUM")
			,tmp_clf_mdm_dedubl_enriched("birth_date")
			,tmp_clf_mdm_dedubl_enriched("birth_place")
			,tmp_clf_mdm_dedubl_enriched("id_end_date")
			,tmp_clf_mdm_dedubl_enriched("gender_tp_code")
			,tmp_clf_mdm_dedubl_enriched("citizenship")
			,tmp_clf_mdm_dedubl_enriched("inn")
			,tmp_clf_mdm_dedubl_enriched("adr_reg")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_postal_code")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_cntry_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_region")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_city_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_area")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_settlement")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_street_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_house_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_korpus_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_building_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_residence_num")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_1")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_2")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_3")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_4")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_5")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_6")
			,tmp_clf_mdm_dedubl_enriched("adr_fact")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_postal_code")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_cntry_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_region")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_city_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_area")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_settlement")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_street_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_house_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_korpus_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_building_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_residence_num")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_1")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_2")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_3")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_4")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_5")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_6")
			,tmp_clf_mdm_dedubl_enriched("tel_home")
			,tmp_clf_mdm_dedubl_enriched("tel_mob")
			,tmp_clf_mdm_dedubl_enriched("email")
			,tmp_clf_mdm_dedubl_enriched("udbo_open_dt")
			,tmp_clf_mdm_dedubl_enriched("udbo_active_flag")
			,tmp_clf_mdm_dedubl_enriched("udbo_agreement_num")
			,tmp_clf_mdm_dedubl_enriched("snils")
			,tmp_clf_mdm_dedubl_enriched("systems_count")
			,tmp_clf_mdm_dedubl_enriched("last_update_dt")
			,tmp_clf_mdm_dedubl_enriched("full_name_clear")
			,tmp_clf_mdm_dedubl_enriched("id_series_num_clear")
			,tmp_clf_mdm_dedubl_enriched("inn_clear")
			,tmp_clf_mdm_dedubl_enriched("birth_date_clear")
			,tmp_clf_mdm_dedubl_enriched("tel_mob_clear")
			,tmp_clf_mdm_dedubl_enriched("rn_inn")
			,tmp_clf_mdm_dedubl_enriched("rn_id")
			,tmp_clf_mdm_dedubl_enriched("rn_tel")
			,tmp_clf_mdm_dedubl_enriched("rn_inn_only")
			,tmp_clf_raw_keys_filtered("k1")
			,tmp_clf_raw_keys_filtered("k2")
			,tmp_clf_raw_keys_filtered("k3")
			,tmp_clf_raw_keys_filtered("k4")
			,tmp_clf_mdm_dedubl_enriched("m1")
			,tmp_clf_mdm_dedubl_enriched("m2")
			,tmp_clf_mdm_dedubl_enriched("m3")
			,tmp_clf_mdm_dedubl_enriched("m4")
			,lit(3).as("mkj")
		)

	private val union_part4 = tmp_clf_mdm_dedubl_enriched.join(broadcast(tmp_clf_raw_keys_filtered), tmp_clf_mdm_dedubl_enriched("m4") === tmp_clf_raw_keys_filtered("k4"))
		.select(
			 tmp_clf_raw_keys_filtered("id")
			,tmp_clf_raw_keys_filtered("position_eks")
			,tmp_clf_raw_keys_filtered("position_ul")
			,tmp_clf_raw_keys_filtered("doc_ser_eks")
			,tmp_clf_raw_keys_filtered("doc_num_eks")
			,tmp_clf_raw_keys_filtered("doc_date_eks")
			,tmp_clf_raw_keys_filtered("crm_id")
			,tmp_clf_raw_keys_filtered("inn_eks")
			,tmp_clf_raw_keys_filtered("inn_ul")
			,tmp_clf_raw_keys_filtered("inn_ex")
			,tmp_clf_raw_keys_filtered("inn_gs")
			,tmp_clf_mdm_dedubl_enriched("mdm_id")
			,tmp_clf_mdm_dedubl_enriched("mdm_active_flag")
			,tmp_clf_mdm_dedubl_enriched("full_name")
			,tmp_clf_mdm_dedubl_enriched("clf_l_name")
			,tmp_clf_mdm_dedubl_enriched("clf_f_name")
			,tmp_clf_mdm_dedubl_enriched("clf_m_name")
			,tmp_clf_mdm_dedubl_enriched("status_active_dead")
			,tmp_clf_mdm_dedubl_enriched("id_series_num")
			,tmp_clf_mdm_dedubl_enriched("id_date")
			,tmp_clf_mdm_dedubl_enriched("identifier_desc")
			,tmp_clf_mdm_dedubl_enriched("issue_location")
			,tmp_clf_mdm_dedubl_enriched("ID_SERIES")
			,tmp_clf_mdm_dedubl_enriched("ID_NUM")
			,tmp_clf_mdm_dedubl_enriched("birth_date")
			,tmp_clf_mdm_dedubl_enriched("birth_place")
			,tmp_clf_mdm_dedubl_enriched("id_end_date")
			,tmp_clf_mdm_dedubl_enriched("gender_tp_code")
			,tmp_clf_mdm_dedubl_enriched("citizenship")
			,tmp_clf_mdm_dedubl_enriched("inn")
			,tmp_clf_mdm_dedubl_enriched("adr_reg")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_postal_code")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_cntry_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_region")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_city_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_area")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_settlement")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_street_name")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_house_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_korpus_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_building_number")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_residence_num")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_1")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_2")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_3")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_4")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_5")
			,tmp_clf_mdm_dedubl_enriched("adr_reg_kladr_6")
			,tmp_clf_mdm_dedubl_enriched("adr_fact")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_postal_code")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_cntry_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_region")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_city_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_area")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_settlement")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_street_name")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_house_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_korpus_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_building_number")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_residence_num")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_1")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_2")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_3")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_4")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_5")
			,tmp_clf_mdm_dedubl_enriched("adr_fact_kladr_6")
			,tmp_clf_mdm_dedubl_enriched("tel_home")
			,tmp_clf_mdm_dedubl_enriched("tel_mob")
			,tmp_clf_mdm_dedubl_enriched("email")
			,tmp_clf_mdm_dedubl_enriched("udbo_open_dt")
			,tmp_clf_mdm_dedubl_enriched("udbo_active_flag")
			,tmp_clf_mdm_dedubl_enriched("udbo_agreement_num")
			,tmp_clf_mdm_dedubl_enriched("snils")
			,tmp_clf_mdm_dedubl_enriched("systems_count")
			,tmp_clf_mdm_dedubl_enriched("last_update_dt")
			,tmp_clf_mdm_dedubl_enriched("full_name_clear")
			,tmp_clf_mdm_dedubl_enriched("id_series_num_clear")
			,tmp_clf_mdm_dedubl_enriched("inn_clear")
			,tmp_clf_mdm_dedubl_enriched("birth_date_clear")
			,tmp_clf_mdm_dedubl_enriched("tel_mob_clear")
			,tmp_clf_mdm_dedubl_enriched("rn_inn")
			,tmp_clf_mdm_dedubl_enriched("rn_id")
			,tmp_clf_mdm_dedubl_enriched("rn_tel")
			,tmp_clf_mdm_dedubl_enriched("rn_inn_only")
			,tmp_clf_raw_keys_filtered("k1")
			,tmp_clf_raw_keys_filtered("k2")
			,tmp_clf_raw_keys_filtered("k3")
			,tmp_clf_raw_keys_filtered("k4")
			,tmp_clf_mdm_dedubl_enriched("m1")
			,tmp_clf_mdm_dedubl_enriched("m2")
			,tmp_clf_mdm_dedubl_enriched("m3")
			,tmp_clf_mdm_dedubl_enriched("m4")
			,lit(4).as("mkj")
		)

	private val unioned = union_part1.union(union_part2).union(union_part3).union(union_part4)


	val dataframe = unioned.select(
		$"id",
		$"position_eks",
		$"position_ul",
		$"doc_ser_eks",
		$"doc_num_eks",
		$"doc_date_eks",
		$"crm_id",
		$"inn_eks",
		$"inn_ul",
		$"inn_ex",
		$"inn_gs",
		$"mdm_id",
		$"mdm_active_flag",
		$"full_name",
		$"clf_l_name",
		$"clf_f_name",
		$"clf_m_name",
		$"status_active_dead",
		$"id_series_num",
		$"id_date",
		$"identifier_desc",
		$"issue_location",
		$"ID_SERIES",
		$"ID_NUM",
		$"birth_date",
		$"birth_place",
		$"id_end_date",
		$"gender_tp_code",
		$"citizenship",
		$"inn",
		$"adr_reg",
		$"adr_reg_postal_code",
		$"adr_reg_cntry_name",
		$"adr_reg_region",
		$"adr_reg_city_name",
		$"adr_reg_area",
		$"adr_reg_settlement",
		$"adr_reg_street_name",
		$"adr_reg_house_number",
		$"adr_reg_korpus_number",
		$"adr_reg_building_number",
		$"adr_reg_residence_num",
		$"adr_reg_kladr",
		$"adr_reg_kladr_1",
		$"adr_reg_kladr_2",
		$"adr_reg_kladr_3",
		$"adr_reg_kladr_4",
		$"adr_reg_kladr_5",
		$"adr_reg_kladr_6",
		$"adr_fact",
		$"adr_fact_postal_code",
		$"adr_fact_cntry_name",
		$"adr_fact_region",
		$"adr_fact_city_name",
		$"adr_fact_area",
		$"adr_fact_settlement",
		$"adr_fact_street_name",
		$"adr_fact_house_number",
		$"adr_fact_korpus_number",
		$"adr_fact_building_number",
		$"adr_fact_residence_num",
		$"adr_fact_kladr",
		$"adr_fact_kladr_1",
		$"adr_fact_kladr_2",
		$"adr_fact_kladr_3",
		$"adr_fact_kladr_4",
		$"adr_fact_kladr_5",
		$"adr_fact_kladr_6",
		$"tel_home",
		$"tel_mob",
		$"email",
		$"udbo_open_dt",
		$"udbo_active_flag",
		$"udbo_agreement_num",
		$"snils",
		$"systems_count",
		$"last_update_dt",
		$"full_name_clear",
		$"id_series_num_clear",
		$"inn_clear",
		$"birth_date_clear",
		$"tel_mob_clear",
		$"rn_inn",
		$"rn_id",
		$"rn_tel",
		$"rn_inn_only",
		$"k1",
		$"k2",
		$"k3" ,
		$"k4",
		$"m1",
		$"m2",
		$"m3",
		$"m4",
		$"mkj"
	)

	def DoClfFpersKiMdm3()
	{
		Logger.getLogger(Nodet_team_k7m_aux_d_clf_fpers_mdm_kiOUT).setLevel(Level.WARN)

		dataframe
			.write.format("parquet")
			.mode(SaveMode.Overwrite)
			.option("path", dashboardPath)
			.saveAsTable(s"$Nodet_team_k7m_aux_d_clf_fpers_mdm_kiOUT")

		logInserted()
	}
}