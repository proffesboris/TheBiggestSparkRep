package ru.sberbank.sdcb.k7m.core.pack
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.time.LocalDate
import java.sql.Date

class CountrStg3Class(val spark: SparkSession, val config: Config) extends EtlLogger with EtlJob {

  val DevSchema = config.aux
  val MartSchema = config.pa

  val Node1t_team_k7m_pa_d_countr_stg3IN = s"${DevSchema}.countr_stg2"
  val Node2t_team_k7m_pa_d_countr_stg3IN = s"${MartSchema}.pdeksin2"
   val Nodet_team_k7m_pa_d_countr_stg3OUT = s"${DevSchema}.countr_stg3"
  val dashboardPath = s"${config.auxPath}countr_stg3"
 

  override val dashboardName: String = Nodet_team_k7m_pa_d_countr_stg3OUT //витрина
  override def processName: String = "check_countr"

  def DoCountrStg3(date: String) {

    Logger.getLogger(Nodet_team_k7m_pa_d_countr_stg3OUT).setLevel(Level.WARN)

    val dateMinus1Yr = Date.valueOf(LocalDate.parse(date).minusYears(1)).toString

     val createHiveTableStage1 = spark.sql(
      s"""select t.*,
                 row_number() over(partition by inn_to order by amt desc) rn
                 from
                 (
                    select
                      p.inn_st as inn_to
                    , p.inn_sec as inn_from
                    , sum(p.c_sum_nt) as amt
                    from $Node1t_team_k7m_pa_d_countr_stg3IN c
                    join $Node2t_team_k7m_pa_d_countr_stg3IN p on p.inn_st = c.inn
                    where 1=1
                    and lower(p.ypred)='оплата по договору'
                    and p.ktdt='0'
                    and p.c_date_prov>= '$dateMinus1Yr'
                    group by p.inn_st, p.inn_sec
                 ) t
       """
    ).write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .option("path", dashboardPath)
      .saveAsTable(s"$Nodet_team_k7m_pa_d_countr_stg3OUT")

    logInserted()
  }
}


