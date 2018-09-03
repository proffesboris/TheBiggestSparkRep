package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class ClfFpersCrmcbKiClass2 (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  import spark.implicits._

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_fpers_crmcb_kiIN = s"${DevSchema}.tmp_clf_crmcb_dedubl"
  val Node2t_team_k7m_aux_d_clf_fpers_crmcb_kiIN = s"${DevSchema}.tmp_clf_raw_keys"
  val Nodet_team_k7m_aux_d_clf_fpers_crmcb_kiOUT = s"${DevSchema}.clf_fpers_crmcb_ki"
  val dashboardPath = s"${config.auxPath}clf_fpers_crmcb_ki"

  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_fpers_crmcb_kiOUT //витрина
  override def processName: String = "CLF"

  private val tmp_clf_crmcb_dedubl = spark.table(Node1t_team_k7m_aux_d_clf_fpers_crmcb_kiIN)
  private val tmp_clf_raw_keys = spark.table(Node2t_team_k7m_aux_d_clf_fpers_crmcb_kiIN)

  private val tmp_clf_crmcb_dedubl_filtered = tmp_clf_crmcb_dedubl.where($"rn_id" === 1 || $"rn_tel" === 1)

  private val tmp_clf_crmcb_dedubl_enriched = tmp_clf_crmcb_dedubl_filtered.select(
    $"crm_id",
    $"full_name",
    $"clf_l_name",
    $"clf_f_name",
    $"clf_m_name",
    $"id_series_num",
    $"id_series",
    $"id_num",
    $"registrator",
    $"reg_code",
    $"id_date",
    $"birth_date",
    $"job",
    $"id_end_date",
    $"gender_tp_code",
    $"tel_mob",
    $"email",
    $"last_update_dt",
    $"full_name_clear",
    $"id_series_num_clear",
    $"birth_date_clear",
    $"tel_mob_clear",
    $"rn_id",
    $"rn_tel"
  ).withColumn(
    "c1", when($"full_name_clear".isNotNull && $"id_series_num_clear".isNotNull && $"birth_date_clear".isNotNull,
      concat($"full_name_clear", lit("_"), $"id_series_num_clear",lit("_"), $"birth_date_clear")).otherwise(lit(null))
  ).withColumn(
    "c2", when($"full_name_clear".isNotNull && $"birth_date_clear".isNotNull && $"tel_mob_clear".isNotNull,
      concat($"full_name_clear", lit("_"), $"birth_date_clear",lit("_"), $"tel_mob_clear")).otherwise(lit(null))
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
    $"keys_cell_ph_num")
    .withColumn(
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

  private val union_part1 = tmp_clf_crmcb_dedubl_enriched.join(broadcast(tmp_clf_raw_keys_filtered), tmp_clf_crmcb_dedubl_enriched("c1") === tmp_clf_raw_keys_filtered("k1"))
    .select(
      tmp_clf_raw_keys_filtered("id")
      ,tmp_clf_raw_keys_filtered("position")
      ,tmp_clf_raw_keys_filtered("keys_inn")
      ,tmp_clf_raw_keys_filtered("position_eks")
      ,tmp_clf_raw_keys_filtered("position_ul")
      ,tmp_clf_raw_keys_filtered("doc_ser_eks")
      ,tmp_clf_raw_keys_filtered("doc_num_eks")
      ,tmp_clf_raw_keys_filtered("doc_date_eks")
      ,tmp_clf_raw_keys_filtered("inn_eks")
      ,tmp_clf_raw_keys_filtered("inn_ul")
      ,tmp_clf_raw_keys_filtered("inn_ex")
      ,tmp_clf_raw_keys_filtered("inn_gs")
      ,tmp_clf_crmcb_dedubl_enriched("crm_id")
      ,tmp_clf_crmcb_dedubl_enriched("full_name")
      ,tmp_clf_crmcb_dedubl_enriched("clf_l_name")
      ,tmp_clf_crmcb_dedubl_enriched("clf_f_name")
      ,tmp_clf_crmcb_dedubl_enriched("clf_m_name")
      ,tmp_clf_crmcb_dedubl_enriched("id_series_num")
      ,tmp_clf_crmcb_dedubl_enriched("id_series")
      ,tmp_clf_crmcb_dedubl_enriched("id_num")
      ,tmp_clf_crmcb_dedubl_enriched("registrator")
      ,tmp_clf_crmcb_dedubl_enriched("reg_code")
      ,tmp_clf_crmcb_dedubl_enriched("id_date")
      ,tmp_clf_crmcb_dedubl_enriched("birth_date")
      ,tmp_clf_crmcb_dedubl_enriched("job")
      ,tmp_clf_crmcb_dedubl_enriched("id_end_date")
      ,tmp_clf_crmcb_dedubl_enriched("gender_tp_code")
      ,tmp_clf_crmcb_dedubl_enriched("tel_mob")
      ,tmp_clf_crmcb_dedubl_enriched("email")
      ,tmp_clf_crmcb_dedubl_enriched("last_update_dt")
      ,tmp_clf_crmcb_dedubl_enriched("full_name_clear")
      ,tmp_clf_crmcb_dedubl_enriched("id_series_num_clear")
      ,tmp_clf_crmcb_dedubl_enriched("birth_date_clear")
      ,tmp_clf_crmcb_dedubl_enriched("tel_mob_clear")
      ,tmp_clf_crmcb_dedubl_enriched("rn_id")
      ,tmp_clf_crmcb_dedubl_enriched("rn_tel")
      ,tmp_clf_raw_keys_filtered("k1")
      ,tmp_clf_raw_keys_filtered("k2")
      ,tmp_clf_crmcb_dedubl_enriched("c1")
      ,tmp_clf_crmcb_dedubl_enriched("c2")
      ,lit(1).as("ckj")
          )

  private val union_part2 = tmp_clf_crmcb_dedubl_enriched.join(broadcast(tmp_clf_raw_keys_filtered), tmp_clf_crmcb_dedubl_enriched("c2") === tmp_clf_raw_keys_filtered("k2"))
    .select(
      tmp_clf_raw_keys_filtered("id")
      ,tmp_clf_raw_keys_filtered("position")
      ,tmp_clf_raw_keys_filtered("keys_inn")
      ,tmp_clf_raw_keys_filtered("position_eks")
      ,tmp_clf_raw_keys_filtered("position_ul")
      ,tmp_clf_raw_keys_filtered("doc_ser_eks")
      ,tmp_clf_raw_keys_filtered("doc_num_eks")
      ,tmp_clf_raw_keys_filtered("doc_date_eks")
      ,tmp_clf_raw_keys_filtered("inn_eks")
      ,tmp_clf_raw_keys_filtered("inn_ul")
      ,tmp_clf_raw_keys_filtered("inn_ex")
      ,tmp_clf_raw_keys_filtered("inn_gs")
      ,tmp_clf_crmcb_dedubl_enriched("crm_id")
      ,tmp_clf_crmcb_dedubl_enriched("full_name")
      ,tmp_clf_crmcb_dedubl_enriched("clf_l_name")
      ,tmp_clf_crmcb_dedubl_enriched("clf_f_name")
      ,tmp_clf_crmcb_dedubl_enriched("clf_m_name")
      ,tmp_clf_crmcb_dedubl_enriched("id_series_num")
      ,tmp_clf_crmcb_dedubl_enriched("id_series")
      ,tmp_clf_crmcb_dedubl_enriched("id_num")
      ,tmp_clf_crmcb_dedubl_enriched("registrator")
      ,tmp_clf_crmcb_dedubl_enriched("reg_code")
      ,tmp_clf_crmcb_dedubl_enriched("id_date")
      ,tmp_clf_crmcb_dedubl_enriched("birth_date")
      ,tmp_clf_crmcb_dedubl_enriched("job")
      ,tmp_clf_crmcb_dedubl_enriched("id_end_date")
      ,tmp_clf_crmcb_dedubl_enriched("gender_tp_code")
      ,tmp_clf_crmcb_dedubl_enriched("tel_mob")
      ,tmp_clf_crmcb_dedubl_enriched("email")
      ,tmp_clf_crmcb_dedubl_enriched("last_update_dt")
      ,tmp_clf_crmcb_dedubl_enriched("full_name_clear")
      ,tmp_clf_crmcb_dedubl_enriched("id_series_num_clear")
      ,tmp_clf_crmcb_dedubl_enriched("birth_date_clear")
      ,tmp_clf_crmcb_dedubl_enriched("tel_mob_clear")
      ,tmp_clf_crmcb_dedubl_enriched("rn_id")
      ,tmp_clf_crmcb_dedubl_enriched("rn_tel")
      ,tmp_clf_raw_keys_filtered("k1")
      ,tmp_clf_raw_keys_filtered("k2")
      ,tmp_clf_crmcb_dedubl_enriched("c1")
      ,tmp_clf_crmcb_dedubl_enriched("c2")
      ,lit(2).as("ckj")
    )

  private val union_part3 = tmp_clf_crmcb_dedubl_enriched.join(broadcast(tmp_clf_raw_keys_filtered), tmp_clf_crmcb_dedubl_enriched("crm_id") === tmp_clf_raw_keys_filtered("crm_id"))
    .select(
           tmp_clf_raw_keys_filtered("id")
          ,tmp_clf_raw_keys_filtered("position")
          ,tmp_clf_raw_keys_filtered("keys_inn")
      ,tmp_clf_raw_keys_filtered("position_eks")
      ,tmp_clf_raw_keys_filtered("position_ul")
      ,tmp_clf_raw_keys_filtered("doc_ser_eks")
      ,tmp_clf_raw_keys_filtered("doc_num_eks")
      ,tmp_clf_raw_keys_filtered("doc_date_eks")
      ,tmp_clf_raw_keys_filtered("inn_eks")
      ,tmp_clf_raw_keys_filtered("inn_ul")
      ,tmp_clf_raw_keys_filtered("inn_ex")
      ,tmp_clf_raw_keys_filtered("inn_gs")
      ,tmp_clf_crmcb_dedubl_enriched("crm_id")
      ,tmp_clf_crmcb_dedubl_enriched("full_name")
      ,tmp_clf_crmcb_dedubl_enriched("clf_l_name")
      ,tmp_clf_crmcb_dedubl_enriched("clf_f_name")
      ,tmp_clf_crmcb_dedubl_enriched("clf_m_name")
      ,tmp_clf_crmcb_dedubl_enriched("id_series_num")
      ,tmp_clf_crmcb_dedubl_enriched("id_series")
      ,tmp_clf_crmcb_dedubl_enriched("id_num")
      ,tmp_clf_crmcb_dedubl_enriched("registrator")
      ,tmp_clf_crmcb_dedubl_enriched("reg_code")
      ,tmp_clf_crmcb_dedubl_enriched("id_date")
      ,tmp_clf_crmcb_dedubl_enriched("birth_date")
      ,tmp_clf_crmcb_dedubl_enriched("job")
      ,tmp_clf_crmcb_dedubl_enriched("id_end_date")
      ,tmp_clf_crmcb_dedubl_enriched("gender_tp_code")
      ,tmp_clf_crmcb_dedubl_enriched("tel_mob")
      ,tmp_clf_crmcb_dedubl_enriched("email")
      ,tmp_clf_crmcb_dedubl_enriched("last_update_dt")
      ,tmp_clf_crmcb_dedubl_enriched("full_name_clear")
      ,tmp_clf_crmcb_dedubl_enriched("id_series_num_clear")
      ,tmp_clf_crmcb_dedubl_enriched("birth_date_clear")
      ,tmp_clf_crmcb_dedubl_enriched("tel_mob_clear")
      ,tmp_clf_crmcb_dedubl_enriched("rn_id")
      ,tmp_clf_crmcb_dedubl_enriched("rn_tel")
          ,tmp_clf_raw_keys_filtered("k1")
          ,tmp_clf_raw_keys_filtered("k2")
      ,tmp_clf_crmcb_dedubl_enriched("c1")
      ,tmp_clf_crmcb_dedubl_enriched("c2")
      ,lit(5).as("ckj")
    )

  private val unioned = union_part1.union(union_part2).union(union_part3)

  val dataframe = unioned.select(
    $"id",
    $"position",
    $"keys_inn",
    $"position_eks",
    $"position_ul",
    $"doc_ser_eks",
    $"doc_num_eks",
    $"doc_date_eks",
    $"inn_eks",
    $"inn_ul",
    $"inn_ex",
    $"inn_gs",
    $"crm_id",
    $"full_name",
    $"clf_l_name",
    $"clf_f_name",
    $"clf_m_name",
    $"id_series_num",
    $"id_series",
    $"id_num",
    $"registrator",
    $"reg_code",
    $"id_date",
    $"birth_date",
    $"job",
    $"id_end_date",
    $"gender_tp_code",
    $"tel_mob",
    $"email",
    $"last_update_dt",
    $"full_name_clear",
    $"id_series_num_clear",
    $"birth_date_clear",
    $"tel_mob_clear",
    $"rn_id",
    $"rn_tel",
    $"k1",
    $"k2",
    $"c1",
    $"c2",
    $"ckj")

  def DoClfFpersCrmcbKi2()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_fpers_crmcb_kiOUT).setLevel(Level.WARN)

    dataframe
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_fpers_crmcb_kiOUT")

    logInserted()
  }
}