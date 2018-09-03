package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

class CarDtClass (val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val Stg0Schema = config.stg
  val DevSchema = config.aux

  val Node1t_team_k7m_aux_d_k7m_car_dtIN = s"${DevSchema}.k7m_prd_dt"
  val Node2t_team_k7m_aux_d_k7m_car_dtIN = s"${DevSchema}.k7m_cred_bal_total"
  val Nodet_team_k7m_aux_d_k7m_car_dtOUT = s"${DevSchema}.k7m_car_dt"
  val dashboardPath = s"${config.auxPath}k7m_car_dt"


  override val dashboardName: String = Nodet_team_k7m_aux_d_k7m_car_dtOUT //витрина
  override def processName: String = "Plan_liab"

  def DoCarDt()
  {
    Logger.getLogger(Nodet_team_k7m_aux_d_k7m_car_dtOUT).setLevel(Level.WARN)
    logStart()

    val createHiveTableStage1 = spark.sql(
      s"""
       select   p.c_client_id
              , p.id, p.c_num_dog
              , cast(ct.od as decimal(37,2)) as od -- -sum(case when r.c_name_account = 2049586 then r.rest else 0 end) rest
              , cast(ct.od_lcl as decimal(37,2)) as od_lcl -- -sum(case when r.c_name_account = 2049586 then r.rest_nat else 0 end) rest_nat
              , cast(ct.lim as decimal(37,2)) as lim--sum(case when r.c_name_account = 2049588 then r.rest else 0 end) lim
              , cast(ct.lim_lcl as decimal(37,2)) as lim_lcl --sum(case when r.c_name_account = 2049588 then r.rest_nat else 0 end) lim_nat
         from $Node1t_team_k7m_aux_d_k7m_car_dtIN p
         left join $Node2t_team_k7m_aux_d_k7m_car_dtIN ct on ct.cred_id = p.id
    """
    )
    createHiveTableStage1
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath).saveAsTable(s"$Nodet_team_k7m_aux_d_k7m_car_dtOUT")

    logInserted()
    logEnd()
  }
}

