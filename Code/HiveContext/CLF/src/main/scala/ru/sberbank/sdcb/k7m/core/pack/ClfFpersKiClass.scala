package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClfFpersKiClass  (val spark: SparkSession, val config: Config)extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_clf_fpers_kiIN = s"${DevSchema}.clf_fpers_crmcb"
  val Node2t_team_k7m_aux_d_clf_fpers_kiIN = s"${DevSchema}.clf_fpers_mdm"
  val Nodet_team_k7m_aux_d_clf_fpers_kiOUT = s"${DevSchema}.clf_fpers_ki"
  val dashboardPath = s"${config.auxPath}clf_fpers_ki"


  override val dashboardName: String = Nodet_team_k7m_aux_d_clf_fpers_kiOUT //витрина
  override def processName: String = "CLF"

  def DoClfFpersKi()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_clf_fpers_kiOUT).setLevel(Level.WARN)

    val smartSrcHiveTableStage1 = spark.sql(
      s"""
          select
         	c.id as id
         	,cast(null as string) as mdm_id
         	,c.crm_id
         	,c.c1  as k1
         	,c.c2  as k2
         	,cast(null as string)  as k3
      --   ,concat(full_name_clear,'_',coalesce(c.inn_eks, c.inn_ex, c.inn_ul, c.inn_gs)) as k3
         	--,cast(null as string)  as k4
          ,coalesce(c.inn_eks, c.inn_ex, c.inn_ul, c.inn_gs) as k4
         	,c.ckj as k
         from
          $Node1t_team_k7m_aux_d_clf_fpers_kiIN c
    """
    )
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_fpers_kiOUT")

    val smartSrcHiveTableStage2 = spark.sql(
      s"""

         select
	         cast(m.id as string) as id
	      ,  cast(m.mdm_id as string) as mdm_id
	      ,	cast(null as string) as crm_id
	      ,   m.m1  as k1
	      ,   m.m2  as k2
	    --  ,	m.m3  as k3
     ,concat(full_name_clear,'_',coalesce(m.m4, m.inn_eks, m.inn_ex, m.inn_ul, m.inn_gs)) as k3
	      --,	m.m4  as k4
       ,coalesce(m.m4, m.inn_eks, m.inn_ex, m.inn_ul, m.inn_gs) as k4
	      ,   m.mkj as k
from $Node2t_team_k7m_aux_d_clf_fpers_kiIN m
    """
    )
      .write.format("parquet")
      .mode(SaveMode.Append)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_clf_fpers_kiOUT")

    logInserted()
  }
}