package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class ClientDebtOdClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_k7m_client_debt_odIN = s"${DevSchema}.k7m_PRD_DT"
  val Node2t_team_k7m_aux_d_k7m_client_debt_odIN = s"${DevSchema}.k7m_car_dt"
  val Node3t_team_k7m_aux_d_k7m_client_debt_odIN = s"${DevSchema}.k7m_cal_dt"
   val Nodet_team_k7m_aux_d_k7m_client_debt_odOUT = s"${DevSchema}.k7m_client_debt_od"
  val dashboardPath = s"${config.auxPath}k7m_client_debt_od"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_client_debt_odOUT //витрина
  override def processName: String = "Plan_liab"

  def DoClientDebtOd()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_client_debt_odOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""
       select p.c_client_id c_client_id
            , p.id id
            ,cast(greatest(coalesce(r.od_lcl,0)+coalesce(r.lim_lcl,0), coalesce(l.max_limit_nat,0)) as decimal(38,2)) as client_debt
            ,r.od_lcl od_lcl
            ,r.lim_lcl lim_lcl
            ,l.max_limit_nat max_limit_nat
            ,p.c_num_dog c_num_dog
            ,p.c_date_begin c_date_begin
            ,p.c_date_ending c_date_ending
            ,p.class_id class_id
            ,p.c_short_name c_short_name
            ,p.c_high_level_cr c_high_level_cr
            from $Node1t_team_k7m_aux_d_k7m_client_debt_odIN p
            left join $Node2t_team_k7m_aux_d_k7m_client_debt_odIN r on r.id = p.id
            left join $Node3t_team_k7m_aux_d_k7m_client_debt_odIN l on l.id = p.id
             where p.id = p.c_high_level_cr
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_client_debt_odOUT")

    logInserted()
    logEnd()
  }
}

