package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfFpersClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_fpersIN = s"${DevSchema}.clf_fpers_mdm"
  val Node2t_team_k7m_aux_d_clf_fpersIN = s"${DevSchema}.clf_fpers_crmcb"
  val Nodet_team_k7m_aux_d_clf_fpersOUT = s"${DevSchema}.clf_fpers"
  val dashboardPath = s"${config.auxPath}clf_fpers"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_fpersOUT //витрина

  def DoClfFpers()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_fpersOUT).setLevel(Level.WARN)
    logStart()

    val smartSrcHiveTable_t7 = spark.sql(
      s"""
         select
         		m.id
         	, m.mdm_id
         	,	c.crm_id
         	,	m.k1
         	,	m.k2
         	,	m.k3
         	,	m.k4
         	,	m.m1
         	,	m.m2
         	,	m.m3
         	,	m.m4
         	,	c.c1
         	,	c.c2
         	,	m.mkj
         	,	c.ckj
        -- 	,	f() as row_hash
         from
         $Node1t_team_k7m_aux_d_clf_fpersIN m
         join $Node2t_team_k7m_aux_d_clf_fpersIN c on c.id =m.id
    """
    )
    smartSrcHiveTable_t7
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_fpersOUT")

    logInserted()
    logEnd()
  }
}

