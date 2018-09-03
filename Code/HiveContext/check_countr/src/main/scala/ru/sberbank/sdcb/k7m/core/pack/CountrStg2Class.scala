package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CountrStg2Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_countr_stg2IN = s"${DevSchema}.basis_client"
  val Node2t_team_k7m_aux_d_countr_stg2IN = s"${Stg0Schema}.crm_s_org_ext_x"
   val Nodet_team_k7m_aux_d_countr_stg2OUT = s"${DevSchema}.countr_stg2"
  val dashboardPath = s"${config.auxPath}countr_stg2"
 

  override val dashboardName: String = Nodet_team_k7m_aux_d_countr_stg2OUT //витрина
  override def processName: String = "check_countr"

  def DoCountrStg2() {

    Logger.getLogger(Nodet_team_k7m_aux_d_countr_stg2OUT).setLevel(Level.WARN)

    val createHiveTableStage1 = spark.sql(
      s"""select b.org_inn_crm_num as inn
             from $Node1t_team_k7m_aux_d_countr_stg2IN b
             join $Node2t_team_k7m_aux_d_countr_stg2IN x on x.par_row_id = b.org_crm_id and x.x_cb_country_id is null
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_countr_stg2OUT")

    logInserted()
  }
}


