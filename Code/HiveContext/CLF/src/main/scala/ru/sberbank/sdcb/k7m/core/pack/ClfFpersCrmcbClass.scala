package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfFpersCrmcbClass (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_fpers_crmcbIN = s"${DevSchema}.clf_fpers_crmcb_ki"
  val Nodet_team_k7m_aux_d_clf_fpers_crmcbOUT = s"${DevSchema}.clf_fpers_crmcb"
  val dashboardPath = s"${config.auxPath}clf_fpers_crmcb"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_fpers_crmcbOUT //витрина
  override def processName: String = "CLF"

  def DoClfFpersCrmcb()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_fpers_crmcbOUT).setLevel(Level.WARN)

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
   select id
         ,position_eks
         ,position_ul
         ,doc_ser_eks
         ,doc_num_eks
         ,doc_date_eks
         ,inn_eks
         ,inn_ul
         ,inn_ex
         ,inn_gs
         ,crm_id
         ,full_name
         ,clf_l_name
         ,clf_f_name
         ,clf_m_name
         ,id_series_num
         ,id_series
         ,id_num
         ,registrator
         ,reg_code
         ,id_date
         ,birth_date
         ,job
         ,id_end_date
         ,gender_tp_code
         ,tel_mob
         ,email
         ,last_update_dt
         ,full_name_clear
         ,id_series_num_clear
         ,birth_date_clear
         ,tel_mob_clear
         ,rn_id
         ,rn_tel
         ,k1
         ,k2
         ,c1
         ,c2
         ,ckj
         from $Node1t_team_k7m_aux_d_clf_fpers_crmcbIN where ckj <> -1
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_fpers_crmcbOUT")

    logInserted()

  }
}