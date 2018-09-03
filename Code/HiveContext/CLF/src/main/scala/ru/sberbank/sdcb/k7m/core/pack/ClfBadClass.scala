package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

class ClfBadClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  import spark.implicits._

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_badIN = s"${DevSchema}.clf_fpers_crmcb"
  val Node2t_team_k7m_aux_d_clf_badIN = s"${DevSchema}.clf_fpers_mdm"
  val Node3t_team_k7m_aux_d_clf_badIN = s"${DevSchema}.tmp_clf_raw_keys"
  val Nodet_team_k7m_aux_d_clf_badOUT = s"${DevSchema}.clf_bad"
  val Nodet_team_k7m_aux_d_clf_badOUT2 = s"${DevSchema}.clf_bad_with_crm"
  val dashboardPath = s"${config.auxPath}clf_bad"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_badOUT //витрина
  override def processName: String = "CLF"

  private val tmp_clf_fpers_crmcb = spark.table(Node1t_team_k7m_aux_d_clf_badIN).select("id")
  private val tmp_clf_fpers_mdm = spark.table(Node2t_team_k7m_aux_d_clf_badIN).select("id")
  private val tmp_clf_raw_keys = spark.table(Node3t_team_k7m_aux_d_clf_badIN)

  private val union_part = tmp_clf_fpers_crmcb.union(tmp_clf_fpers_mdm)

  val tmp_clf_bad = tmp_clf_raw_keys.join(broadcast(union_part), union_part("id") === tmp_clf_raw_keys("id"), "left_anti")
    .select(
       tmp_clf_raw_keys("id")
      ,tmp_clf_raw_keys("position")
      ,tmp_clf_raw_keys("crm_id")
      ,tmp_clf_raw_keys("eks_id")
      ,tmp_clf_raw_keys("inn")
      ,tmp_clf_raw_keys("keys_fio")
      ,tmp_clf_raw_keys("fio").as("fio_src")
      ,tmp_clf_raw_keys("keys_inn")
      ,tmp_clf_raw_keys("keys_doc_ser")
      ,tmp_clf_raw_keys("doc_type")
      ,tmp_clf_raw_keys("doc_ser")
      ,tmp_clf_raw_keys("doc_num")
      ,tmp_clf_raw_keys("doc_date")
      ,tmp_clf_raw_keys("cell_ph_num")
      ,tmp_clf_raw_keys("keys_cell_ph_num")
      ,tmp_clf_raw_keys("keys_birth_dt")
      ,tmp_clf_raw_keys("k1")
      ,tmp_clf_raw_keys("k2")
      ,tmp_clf_raw_keys("k3")
      ,tmp_clf_raw_keys("k4")
    ).withColumn(
    "position_eks", when($"id".like("EK%"),$"position").otherwise(lit(null))
    ).withColumn(
    "position_ul", when($"id".like("UL%"),$"position").otherwise(lit(null))
    )
//    .withColumn(
//      "doc_ser_eks", when($"id".like("EK%"),$"doc_ser").otherwise(lit(null)))
//    .withColumn(
//      "doc_num_eks", when($"id".like("EK%"),$"doc_num").otherwise(lit(null)))
//    .withColumn(
//      "doc_date_eks", when($"id".like("EK%"),$"doc_date").otherwise(lit(null)))

  val dataframe = tmp_clf_bad.where($"crm_id".isNull)
  val dataframe2 = tmp_clf_bad.where($"crm_id".isNotNull)

  def DoClfBad()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_badOUT).setLevel(Level.WARN)

    dataframe
    .write.format("parquet")
    .mode(SaveMode.Overwrite)
    .option("path", dashboardPath)
    .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_badOUT")

    dataframe2
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", s"${config.auxPath}clf_bad_with_crm")
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_badOUT2")

    logInserted()

  }
}
