package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class ClfBadFilteredClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  import spark.implicits._

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_bad_filteredIN = s"${DevSchema}.clf_bad"
  val Node2t_team_k7m_aux_d_clf_bad_filteredIN = s"${DevSchema}.clf_bad_with_crm"
  val Nodet_team_k7m_aux_d_clf_bad_filteredOUT = s"${DevSchema}.clf_bad_filtered"
  val Nodet_team_k7m_aux_d_clf_bad_filteredOUT2 = s"${DevSchema}.clf_bad_without_keys"
  val dashboardPath = s"${config.auxPath}clf_bad_filtered"

  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_bad_filteredOUT //витрина
  override def processName: String = "CLF"

  private val tmp_clf_bad = spark.table(Node1t_team_k7m_aux_d_clf_bad_filteredIN)
  private val tmp_clf_bad_with_crm = spark.table(Node2t_team_k7m_aux_d_clf_bad_filteredIN)

  private val union_part = tmp_clf_bad.union(tmp_clf_bad_with_crm)

  private val tmp_clf_raw_keys_fil_inn = union_part
    .select($"id"
      ,$"position"
      ,$"crm_id"
      ,$"eks_id"
      ,$"keys_fio"
      ,$"keys_inn"
      ,$"keys_doc_ser"
      ,$"doc_ser"
      ,$"doc_num"
      ,$"doc_date"
      ,$"cell_ph_num"
      ,$"keys_cell_ph_num"
      ,$"keys_birth_dt"
      ,$"k1"
      ,$"k2"
      ,$"k3"
      ,$"k4"
      ,$"position_eks"
      ,$"position_ul"
    ).withColumn(
    "inn", when($"keys_inn".isNotNull && length($"keys_inn") === 12 && !$"keys_inn".isin("111111111111","100000000000") && !$"keys_inn".like("000%"),$"keys_inn").otherwise(lit(null)))
    .withColumn(
      "k3_prep", when($"keys_inn".isNotNull && length($"keys_inn") === 12 && !$"keys_inn".isin("111111111111","100000000000") && !$"keys_inn".like("000%"),$"k3").otherwise(lit(null))
    ).withColumn(
    "k4_prep", when($"keys_inn".isNotNull && length($"keys_inn") === 12 && !$"keys_inn".isin("111111111111","100000000000") && !$"keys_inn".like("000%"),$"k4").otherwise(lit(null))
  )

  private val tmp_clf_bad_filter = tmp_clf_raw_keys_fil_inn
    .select($"id"
      ,$"position"
      ,$"crm_id"
      ,$"eks_id"
      ,$"inn"
      ,$"keys_fio"
      ,$"keys_inn"
      ,$"keys_doc_ser"
      ,$"doc_ser"
      ,$"doc_num"
      ,$"doc_date"
      ,$"cell_ph_num"
      ,$"keys_cell_ph_num"
      ,$"keys_birth_dt"
      ,$"k1"
      ,$"k2"
      ,$"k3_prep".as("k3")
      ,$"k4_prep".as("k4")
      ,$"position_eks"
      ,$"position_ul")

  private val tmp_clf_raw_keys_filtered_k = tmp_clf_bad_filter.where($"k1".isNotNull || $"k2".isNotNull || $"k3".isNotNull || $"k4".isNotNull || $"crm_id".isNotNull)

  val dataframe = tmp_clf_raw_keys_filtered_k
  val dataframe2 = union_part.join(broadcast(tmp_clf_raw_keys_filtered_k), tmp_clf_raw_keys_filtered_k("id") === union_part("id"), "left_anti")

  def DoClfBadFiltered()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_bad_filteredOUT).setLevel(Level.WARN)

    dataframe
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_bad_filteredOUT")

    dataframe2
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.auxPath}clf_bad_without_keys")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_bad_filteredOUT2")

    logInserted()
  }
}