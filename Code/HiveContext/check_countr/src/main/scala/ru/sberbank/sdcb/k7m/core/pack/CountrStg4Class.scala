package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CountrStg4Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_aux_d_countr_stg4IN = s"${DevSchema}.countr_stg3"
  val Node2t_team_k7m_aux_d_countr_stg4IN = s"${MartSchema}.clu"
   val Nodet_team_k7m_aux_d_countr_stg4OUT = s"${DevSchema}.countr_stg4"
  val dashboardPath = s"${config.auxPath}countr_stg4"
 

  override val dashboardName: String = Nodet_team_k7m_aux_d_countr_stg4OUT //витрина
  override def processName: String = "check_countr"

  def DoCountrStg4() {

    Logger.getLogger(Nodet_team_k7m_aux_d_countr_stg4OUT).setLevel(Level.WARN)

     val createHiveTableStage1 = spark.sql(
      s"""select
                t.*,
                c.reg_country
                from
                $Node1t_team_k7m_aux_d_countr_stg4IN t
                join $Node2t_team_k7m_aux_d_countr_stg4IN c on t.inn_from = c.inn
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_countr_stg4OUT")

    logInserted()
  }
}


