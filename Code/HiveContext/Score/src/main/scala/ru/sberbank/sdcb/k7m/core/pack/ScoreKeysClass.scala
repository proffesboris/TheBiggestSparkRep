package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ScoreKeysClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_score_keysIN = s"${MartSchema}.pdout"
  val Node2t_team_k7m_aux_d_score_keysIN = s"${MartSchema}.clu"
   val Nodet_team_k7m_aux_d_score_keysOUT = s"${MartSchema}.score_keys"
  val dashboardPath = s"${config.paPath}score_keys"


  override val dashboardName: String = Nodet_team_k7m_aux_d_score_keysOUT //витрина
  override def processName: String = "score_keys"

  def DoScoreKeys()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_score_keysOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""
         select distinct
                 cast(pd.exec_id as string) as exec_id,
                 cast(null as string) as okr_id,
                 cast(pd.u7m_id as string) as u7m_id
         from
                 $Node1t_team_k7m_aux_d_score_keysIN pd
                 join $Node2t_team_k7m_aux_d_score_keysIN bc on (pd.u7m_id = bc.u7m_id)
         where
              		bc.flag_basis_client = 'Y'
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_score_keysOUT")

    logInserted()
    logEnd()
  }
}
