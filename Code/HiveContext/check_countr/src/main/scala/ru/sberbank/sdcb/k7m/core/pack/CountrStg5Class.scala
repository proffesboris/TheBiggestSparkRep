package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CountrStg5Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_countr_stg5IN = s"${DevSchema}.countr_stg4"
   val Nodet_team_k7m_aux_d_countr_stg5OUT = s"${DevSchema}.countr_stg5"
  val dashboardPath = s"${config.auxPath}countr_stg5"
 

  override val dashboardName: String = Nodet_team_k7m_aux_d_countr_stg5OUT //витрина
  override def processName: String = "check_countr"

  def DoCountrStg5() {

    Logger.getLogger(Nodet_team_k7m_aux_d_countr_stg5OUT).setLevel(Level.WARN)

     val createHiveTableStage1 = spark.sql(
      s"""
          select t.* from
          (select   t.*
                 , row_number() over(partition by inn_to order by s1 desc) rn
                 from
                 (
                    select inn_to, reg_country, sum(amt) s1
                    from $Node1t_team_k7m_aux_d_countr_stg5IN
                    GROUP BY inn_to, reg_country
                 ) t ) t where rn = 1
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_countr_stg5OUT")

    logInserted()

  }
}


