package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TmpClfMdmCntryClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_tmp_clf_mdm_cntryIN = s"${Stg0Schema}.mdm_cdcountrytp"
   val Nodet_team_k7m_aux_d_tmp_clf_mdm_cntryOUT = s"${DevSchema}.tmp_clf_mdm_cntry"
  val dashboardPath = s"${config.auxPath}tmp_clf_mdm_cntry"


  override val dashboardName: String = Nodet_team_k7m_aux_d_tmp_clf_mdm_cntryOUT //витрина
  override def processName: String = "CLF"

  def DoTmpClfMdmCntry()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_tmp_clf_mdm_cntryOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""			select
         		  t.country_tp_cd	-- id в справочнике
         		, t.name			-- Наименование страны
         		, t.description	-- Цифровой международный код страны
         		, t.iso_code		-- Буквенный ISO-код страны
         	from $Node1t_team_k7m_aux_d_tmp_clf_mdm_cntryIN t
         	where t.lang_tp_cd = 2200
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_tmp_clf_mdm_cntryOUT")

    logInserted()
  }
}