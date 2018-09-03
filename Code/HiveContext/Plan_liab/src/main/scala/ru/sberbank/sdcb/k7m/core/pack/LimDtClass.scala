package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class LimDtClass (val spark: SparkSession, val config: Config, val date: String) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_k7m_lim_dtIN = s"${DevSchema}.k7m_prd_dt"
  val Node2t_team_k7m_aux_d_k7m_lim_dtIN = s"${Stg0Schema}.eks_Z_LIMIT_HISTORY"
   val Nodet_team_k7m_aux_d_k7m_lim_dtOUT = s"${DevSchema}.k7m_lim_dt"
  val dashboardPath = s"${config.auxPath}k7m_lim_dt"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_lim_dtOUT //витрина
  override def processName: String = "Plan_liab"

  def DolimDt()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_lim_dtOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""
       select  id
              ,collection_id
              ,c_date_begin
              ,c_date_end
              ,c_summa
         from (
         select
           cast(p.id as decimal(38,2)) as id
         , cast(l.collection_id as decimal(38,2)) as collection_id
         , l.c_date_begin
         , coalesce(l.c_date_end,  cast('9999-12-31' as timestamp)) as c_date_end
         , cast(l.c_summa as decimal(38,2)) as c_summa
                from $Node1t_team_k7m_aux_d_k7m_lim_dtIN p
                join (select collection_id, c_date_begin, c_date_end, c_summa
                                       from $Node2t_team_k7m_aux_d_k7m_lim_dtIN
                     ) l on l.collection_id = p.c_limit_history
         ) t where t.c_date_end >=  cast('$date' as timestamp)
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_lim_dtOUT")

    logInserted()
    logEnd()
  }
}

