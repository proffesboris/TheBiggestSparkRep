package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CountrStg1Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_countr_stg1IN = s"${DevSchema}.basis_client"
  val Node2t_team_k7m_aux_d_countr_stg1IN = s"${Stg0Schema}.crm_s_org_ext_x"
   val Nodet_team_k7m_aux_d_countr_stg1OUT = s"${DevSchema}.countr_stg1"
  val dashboardPath = s"${config.auxPath}countr_stg1"
 

  override val dashboardName: String = Nodet_team_k7m_aux_d_countr_stg1OUT //витрина
  override def processName: String = "check_countr"

  def DoCountrStg1() {

    Logger.getLogger(Nodet_team_k7m_aux_d_countr_stg1OUT).setLevel(Level.WARN)

     val createHiveTableStage1 = spark.sql(
      s"""select  org_crm_id as crm_id
                 ,case when x.x_cb_country_id in ('1-1YUF2JG') then 0 else 1 end as flag_countr
                 ,x_cb_country_id
                 from $Node1t_team_k7m_aux_d_countr_stg1IN b
                 join $Node2t_team_k7m_aux_d_countr_stg1IN x on x.par_row_id = b.org_crm_id and x.x_cb_country_id is not null
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_aux_d_countr_stg1OUT")

    logInserted()

  }
}


